#!/usr/bin/env python3
import os
import sys
import subprocess
import uuid
import json
import time
import threading
import signal
import atexit
import logging
import argparse
import tempfile
import re

import requests
from flask import Flask, render_template_string, request, jsonify
from flask_apscheduler import APScheduler

# ── Load .env file from script directory ─────────────────────────────

def _load_dotenv():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.isfile(env_path):
        return
    try:
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key not in os.environ:
                    os.environ[key] = val
    except OSError:
        pass

_load_dotenv()

# ── Logging ──────────────────────────────────────────────────────────

log = logging.getLogger("aria2-webui")


# ── Config from env vars with sane defaults ──────────────────────────

def _env_int(key, default):
    val = os.environ.get(key, "")
    if not val:
        return default
    try:
        return int(val)
    except ValueError:
        log.error("Invalid integer for %s=%r — using default %d", key, val, default)
        return default


def _env_bool(key, default):
    val = os.environ.get(key, "")
    if not val:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


_ARIA2_RPC = os.environ.get("ARIA2_RPC", "http://localhost:6800/jsonrpc")
_ARIA2_SECRET = os.environ.get("ARIA2_SECRET", "")
_ARIA2_PORT = _env_int("ARIA2_PORT", 6800)
_DB_FILE = os.environ.get("DB_FILE", "aria_tasks.json")
_DOWNLOAD_STALL_SECONDS = _env_int("DOWNLOAD_STALL_SECONDS", 300)
# Parallel connections per server — bypasses per-connection speed throttling
# (tunable from the web UI, persisted to _DB_FILE)
_SETTINGS = {
    "connections_per_server": _env_int("CONNECTIONS_PER_SERVER", 16),
    "min_split_size": os.environ.get("MIN_SPLIT_SIZE", "8M"),
    "file_allocation": os.environ.get("FILE_ALLOCATION", "none").strip().lower(),
    "max_download_limit": os.environ.get("MAX_DOWNLOAD_LIMIT", "0").strip().upper(),
    "max_tries": _env_int("MAX_TRIES", 5),
    "retry_wait": _env_int("RETRY_WAIT", 0),
    "check_certificate": _env_bool("CHECK_CERTIFICATE", False),
    "connect_timeout": _env_int("CONNECT_TIMEOUT", 60),
    "timeout": _env_int("TIMEOUT", 60),
    "user_agent": os.environ.get("USER_AGENT", ""),
    "referer": os.environ.get("REFERER", ""),
    "remote_time": _env_bool("REMOTE_TIME", False),
}

# Reuse one TCP connection for the frequent aria2 RPC polls. A lock keeps the
# shared session safe when the download worker and health checker overlap.
_RPC_SESSION = requests.Session()
_RPC_LOCK = threading.Lock()

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 2 * 1024 * 1024  # 2 MB
scheduler = APScheduler()

# ── Thread safety ────────────────────────────────────────────────────

_status_lock = threading.Lock()
_supervisor_lock = threading.Lock()
_file_lock = threading.Lock()
_cron_lock = threading.Lock()           # protects job_cron_params + scheduler mutations
_active_gids_lock = threading.Lock()
_shutdown_flag = threading.Event()

# ── In-memory state ──────────────────────────────────────────────────

job_status = {}             # {jid: {status, progress, …}}
active_gids = {}            # {jid: aria2_gid}
job_cron_params = {}        # {jid: {"hour": h, "minute": m, "is_now": bool}} — avoids trigger internals
_aria2_conf_path = None     # path to temp aria2.conf (so secret stays off cmdline)
_aria2_healthy = False


# ══════════════════════════════════════════════════════════════════════
#  ARIA2  RPC  HELPERS
# ══════════════════════════════════════════════════════════════════════

def _rpc(method, params=None):
    payload = {
        "jsonrpc": "2.0",
        "id": "aria2",
        "method": method,
        "params": [f"token:{_ARIA2_SECRET}"] + (params or []),
    }
    try:
        with _RPC_LOCK:
            r = _RPC_SESSION.post(_ARIA2_RPC, json=payload, timeout=5)
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            log.error("RPC error [%s]: %s", method, data["error"])
            return None
        return data.get("result")
    except requests.RequestException as e:
        log.error("RPC request failed [%s]: %s", method, e)
        return None


def _aria2_version():
    return _rpc("aria2.getVersion")


# ── Daemon startup with retry loop ───────────────────────────────────

def _start_aria2_daemon():
    global _aria2_conf_path, _aria2_healthy

    if _aria2_version():
        log.info("aria2 RPC daemon already running.")
        _aria2_healthy = True
        return

    fd, _aria2_conf_path = tempfile.mkstemp(suffix=".conf", prefix="aria2_webui_")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(f"rpc-secret={_ARIA2_SECRET}\n")
            f.write(f"rpc-listen-port={_ARIA2_PORT}\n")
            f.write("rpc-listen-all=false\n")
            f.write("check-certificate=false\n")
            f.write(f"split={_SETTINGS['connections_per_server']}\n")
            f.write(f"max-connection-per-server={_SETTINGS['connections_per_server']}\n")
            f.write(f"min-split-size={_SETTINGS['min_split_size']}\n")
            f.write("continue=false\n")
            f.write("allow-overwrite=true\n")
            f.write("auto-file-renaming=false\n")
    except OSError:
        log.error("Failed to write aria2 config. Does the temp directory exist?")
        return

    log.info("Starting aria2c daemon …")
    try:
        subprocess.Popen(
            ["aria2c", "--enable-rpc", f"--conf-path={_aria2_conf_path}", "--daemon=true"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        log.error("aria2c not found. Install aria2 and ensure it is in PATH.")
        return

    for attempt in range(10):
        time.sleep(0.5)
        if _aria2_version():
            log.info("aria2 RPC daemon started (attempt %d).", attempt + 1)
            _aria2_healthy = True
            return
    log.error("aria2 daemon did not respond within 5 seconds.")


def _apply_settings(data):
    """Validate + merge new settings into the runtime _SETTINGS dict."""
    if not isinstance(data, dict):
        return

    def as_bool(val):
        if isinstance(val, bool):
            return val
        return str(val).strip().lower() in ("1", "true", "yes", "on")

    try:
        conns = int(data.get("connections_per_server",
                            _SETTINGS["connections_per_server"]))
        if 1 <= conns <= 64:
            _SETTINGS["connections_per_server"] = conns
    except (TypeError, ValueError):
        pass
    try:
        mss = str(data.get("min_split_size", _SETTINGS["min_split_size"])).strip()
        if re.fullmatch(r"\d+[MKG]", mss, re.IGNORECASE):
            _SETTINGS["min_split_size"] = mss.upper()
    except (TypeError, ValueError):
        pass

    alloc = str(data.get("file_allocation", _SETTINGS["file_allocation"])).strip().lower()
    if alloc in ("none", "prealloc", "falloc", "trunc"):
        _SETTINGS["file_allocation"] = alloc

    limit = str(data.get("max_download_limit", _SETTINGS["max_download_limit"])).strip().upper()
    if re.fullmatch(r"(0|\d+[KMG])", limit):
        _SETTINGS["max_download_limit"] = limit

    for key, low, high in (("max_tries", 0, 64), ("retry_wait", 0, 600),
                           ("connect_timeout", 1, 600), ("timeout", 1, 600)):
        try:
            num = int(data.get(key, _SETTINGS[key]))
            if low <= num <= high:
                _SETTINGS[key] = num
        except (TypeError, ValueError):
            pass

    for key in ("check_certificate", "remote_time"):
        if key in data:
            _SETTINGS[key] = as_bool(data[key])

    for key in ("user_agent", "referer"):
        if key in data:
            _SETTINGS[key] = str(data[key] or "").strip()



# ── Health check (called periodically by supervisor) ─────────────────

def _check_aria2_health():
    global _aria2_healthy
    if _aria2_version():
        _aria2_healthy = True
        return True
    _aria2_healthy = False
    log.warning("aria2 RPC health check failed.")
    return False


def _add_uri(url, folder):
    options = {
        "dir": folder,
        "continue": "false",
        "allow-overwrite": "true",
        "auto-file-renaming": "false",
        "split": str(_SETTINGS["connections_per_server"]),
        "max-connection-per-server": str(_SETTINGS["connections_per_server"]),
        "min-split-size": _SETTINGS["min_split_size"],
        "file-allocation": _SETTINGS["file_allocation"],
        "max-download-limit": _SETTINGS["max_download_limit"],
        "max-tries": str(_SETTINGS["max_tries"]),
        "retry-wait": str(_SETTINGS["retry_wait"]),
        "check-certificate": "true" if _SETTINGS["check_certificate"] else "false",
        "connect-timeout": str(_SETTINGS["connect_timeout"]),
        "timeout": str(_SETTINGS["timeout"]),
        "remote-time": "true" if _SETTINGS["remote_time"] else "false",
    }
    if _SETTINGS["user_agent"]:
        options["user-agent"] = _SETTINGS["user_agent"]
    if _SETTINGS["referer"]:
        options["referer"] = _SETTINGS["referer"]
    return _rpc("aria2.addUri", [[url], options])


# ── Validation ───────────────────────────────────────────────────────

_URL_RE = re.compile(r"^https?://", re.IGNORECASE)


def _is_valid_url(url):
    return bool(_URL_RE.match(url))


def _url_to_filename(url):
    """Extract a plausible filename from a URL."""
    name = url.split("?")[0].split("/")[-1]
    return name if name else None


def _check_already_downloaded(url, folder):
    filename = _url_to_filename(url)
    if not filename:
        return False
    filepath = os.path.join(folder, filename)
    return os.path.exists(filepath) and not os.path.exists(filepath + ".aria2")


# ── Formatting ───────────────────────────────────────────────────────

def _format_speed(bps):
    if bps >= 1024 * 1024:
        return f"{bps / (1024 * 1024):.1f} MB/s"
    if bps >= 1024:
        return f"{bps / 1024:.1f} KB/s"
    return f"{bps} B/s"


def _format_bytes(b):
    if b >= 1024 ** 3:
        return f"{b / 1024 ** 3:.1f} GB"
    if b >= 1024 ** 2:
        return f"{b / 1024 ** 2:.1f} MB"
    if b >= 1024:
        return f"{b / 1024:.1f} KB"
    return f"{b} B"


# ══════════════════════════════════════════════════════════════════════
#  PERSISTENCE
# ══════════════════════════════════════════════════════════════════════

def _save_tasks():
    with _file_lock, _cron_lock:
        tasks = []
        for job in scheduler.get_jobs():
            jid = job.id
            with _status_lock:
                info = job_status.get(jid, {})
            cron = job_cron_params.get(jid, {})
            tasks.append({
                "id": jid,
                "urls": job.args[0],
                "folder": job.args[1],
                "hour": cron.get("hour", 0),
                "minute": cron.get("minute", 0),
                "is_now": cron.get("is_now", False),
                "paused": job.next_run_time is None,
                "created_at": job.args[2],
                "failed_urls": info.get("failed", []),
                "dl_paused": info.get("dl_paused", False),
            })
        try:
            tmp = _DB_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump({"settings": _SETTINGS, "tasks": tasks}, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, _DB_FILE)
        except OSError as e:
            log.error("Failed to save tasks: %s", e)


def _load_tasks():
    if not os.path.exists(_DB_FILE):
        return
    try:
        with open(_DB_FILE, "r") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.error("Corrupted task file: %s", e)
        return

    if isinstance(data, dict):
        tasks = data.get("tasks", [])
        _apply_settings(data.get("settings", {}))
    else:
        tasks = data

    for t in tasks:
        try:
            hour = int(t["hour"])
            minute = int(t["minute"])
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError(f"Invalid time {hour}:{minute:02d}")
        except (KeyError, ValueError, TypeError) as e:
            log.warning("Skipping task %s: bad time – %s", t.get("id", "?"), e)
            continue

        created_at = t.get("created_at", time.time())
        with _cron_lock:
            scheduler.add_job(
                id=t["id"],
                func=_queue_task,
                trigger="cron",
                hour=hour,
                minute=minute,
                args=[t["urls"], t["folder"], created_at],
            )
            if t.get("paused"):
                scheduler.pause_job(t["id"])
        with _cron_lock:
            job_cron_params[t["id"]] = {"hour": hour, "minute": minute, "is_now": bool(t.get("is_now"))}

        dl_paused = bool(t.get("dl_paused", False))
        status = "queued" if (t.get("is_now") and not t.get("paused")) else (
            "failed" if t.get("failed_urls") else ("dl_paused" if dl_paused else "waiting")
        )
        with _status_lock:
            job_status[t["id"]] = {
                "is_now": bool(t.get("is_now")),
                "dl_paused": dl_paused,
                "status": status,
                "current_url": "",
                "current_file": "",
                "speed": "0 B/s",
                "progress": 0,
                "size": "",
                "done": [],
                "failed": t.get("failed_urls", []),
            }
    log.info("Loaded %d task(s) from %s.", len(tasks), _DB_FILE)


# ══════════════════════════════════════════════════════════════════════
#  DOWNLOAD WORKER
# ══════════════════════════════════════════════════════════════════════

def _wait_for_gid(gid, job_id, url):
    """Poll aria2.tellStatus until download finishes, errors, or is removed.
    Returns True on success, False otherwise.  Detects stalls."""
    last_completed = -1
    stall_start = None

    while not _shutdown_flag.is_set():
        with _status_lock:
            if job_id not in job_status:
                return False

        result = _rpc("aria2.tellStatus", [gid])
        if not result:
            time.sleep(1)
            continue

        status = result.get("status")
        if status is None:
            time.sleep(0.5)
            continue

        total     = int(result.get("totalLength") or 0)
        completed = int(result.get("completedLength") or 0)
        speed     = int(result.get("downloadSpeed") or 0)
        files     = result.get("files", [{}])
        filename = os.path.basename(files[0].get("path", url)) if files else url
        percent = round((completed / total) * 100, 1) if total > 0 else 0

        # Stall detection (only while "active")
        if status == "active" and total > 0:
            if completed == last_completed:
                if stall_start is None:
                    stall_start = time.time()
                elif time.time() - stall_start > _DOWNLOAD_STALL_SECONDS:
                    log.warning("[%s] Download stalled for %s s — aborting: %s",
                                job_id, _DOWNLOAD_STALL_SECONDS, url)
                    _rpc("aria2.remove", [gid])
                    return False
            else:
                stall_start = None
                last_completed = completed

        # Pause handling
        if status == "paused":
            with _status_lock:
                if job_id not in job_status:
                    return False
                job_status[job_id].update({
                    "status": "paused",
                    "dl_paused": True,
                    "current_url": url,
                    "current_file": filename,
                    "speed": "0 B/s",
                    "progress": percent,
                    "size": f"{_format_bytes(completed)} / {_format_bytes(total)}" if total > 0 else "Unknown size",
                })
            time.sleep(0.8)
            continue

        with _status_lock:
            if job_id not in job_status:
                return False
            # Transition out of dl_paused state when aria2 is active again
            if job_status[job_id].get("dl_paused") and status == "active":
                job_status[job_id]["dl_paused"] = False
                job_status[job_id]["status"] = "running"
            job_status[job_id].update({
                "current_url": url,
                "current_file": filename,
                "speed": _format_speed(speed),
                "progress": percent,
                "size": f"{_format_bytes(completed)} / {_format_bytes(total)}" if total > 0 else "Unknown size",
            })

        if status == "complete":
            return True
        if status == "error":
            log.error("[FAIL] %s: %s", url, result.get("errorMessage", ""))
            return False
        if status == "removed":
            return False
        time.sleep(0.5)

    return False  # shutdown requested


def _run_job(job_id, urls, folder):
    os.makedirs(folder, exist_ok=True)
    with _status_lock:
        if job_id in job_status:
            retry_set = set(job_status[job_id].pop("_retry_only", []))
            # When retrying a single link, keep the other failures so they stay
            # visible and retryable instead of silently disappearing.
            other_failed = [
                u for u in job_status[job_id].get("failed", [])
                if not retry_set or u not in retry_set
            ]
            job_status[job_id].update({
                "status": "running",
                "dl_paused": False,
                "current_url": "",
                "current_file": "",
                "speed": "0 B/s",
                "progress": 0,
                "size": "",
                "done": job_status[job_id].get("done", []),
                "failed": other_failed,
            })

    for url in urls:
        if _shutdown_flag.is_set():
            break
        if retry_set and url not in retry_set:
            continue
        with _status_lock:
            if job_id not in job_status:
                break

        if _check_already_downloaded(url, folder):
            with _status_lock:
                if job_id in job_status and url not in job_status[job_id].get("done", []):
                    job_status[job_id]["done"].append(url)
            continue

        with _status_lock:
            if job_id in job_status and url in job_status[job_id].get("done", []):
                continue

        if not _is_valid_url(url):
            log.warning("[%s] Skipping invalid URL: %s", job_id, url)
            with _status_lock:
                if job_id in job_status:
                    job_status[job_id]["failed"].append(url)
            continue

        gid = _add_uri(url, folder)
        if not gid:
            with _status_lock:
                if job_id in job_status:
                    job_status[job_id]["failed"].append(url)
            continue

        with _active_gids_lock:
            active_gids[job_id] = gid

        success = _wait_for_gid(gid, job_id, url)
        with _status_lock:
            if job_id not in job_status:
                with _active_gids_lock:
                    active_gids.pop(job_id, None)
                break
            if success:
                job_status[job_id]["done"].append(url)
            else:
                job_status[job_id]["failed"].append(url)

        with _active_gids_lock:
            active_gids.pop(job_id, None)

    should_finalize = False
    with _status_lock:
        if job_id in job_status:
            job_status[job_id].update({
                "current_url": "", "current_file": "",
                "speed": "0 B/s", "progress": 0, "size": "", "dl_paused": False,
            })
            if job_status[job_id]["failed"]:
                job_status[job_id]["status"] = "failed"
            else:
                job_status[job_id]["status"] = "done"
                should_finalize = True

    if should_finalize:
        with _cron_lock:
            try:
                scheduler.remove_job(job_id)
            except Exception as e:
                log.warning("Failed to remove completed job %s: %s", job_id, e)
            job_cron_params.pop(job_id, None)
        with _status_lock:
            job_status.pop(job_id, None)

    _save_tasks()


# ── Supervisor ───────────────────────────────────────────────────────

_LAST_HEALTH_CHECK = 0.0


def _download_supervisor():
    global _LAST_HEALTH_CHECK, _aria2_healthy
    while not _shutdown_flag.wait(1.0):
        now = time.time()
        if now - _LAST_HEALTH_CHECK > 30:
            healthy = _check_aria2_health()
            if not healthy:
                log.warning("aria2 RPC unresponsive — attempting restart …")
                _start_aria2_daemon()
            _LAST_HEALTH_CHECK = now

        with _supervisor_lock:
            with _status_lock:
                is_running = any(s.get("status") == "running" for s in job_status.values())

            if not is_running:
                with _status_lock:
                    queued = [
                        (jid, job_status[jid])
                        for jid, s in job_status.items()
                        if s.get("status") == "queued"
                    ]

                if queued:
                    # Sort by created_at (stored in scheduler job args[2])
                    queued_sorted = []
                    for jid, _ in queued:
                        job = scheduler.get_job(jid)
                        if job:
                            queued_sorted.append((jid, job.args[0], job.args[1], job.args[2]))
                    queued_sorted.sort(key=lambda x: x[3])

                    if queued_sorted:
                        jid, urls, folder, _ = queued_sorted[0]
                        with _status_lock:
                            if job_status.get(jid, {}).get("status") == "queued":
                                job_status[jid]["status"] = "running"
                                job_status[jid]["dl_paused"] = False
                        threading.Thread(
                            target=_run_job, args=(jid, urls, folder), daemon=True
                        ).start()


def _queue_task(urls, folder, created_at):
    for job in scheduler.get_jobs():
        if job.args[1] == folder and job.args[2] == created_at:
            with _status_lock:
                if job.id in job_status:
                    job_status[job.id]["status"] = "queued"
                    job_status[job.id]["dl_paused"] = False
            return


# ══════════════════════════════════════════════════════════════════════
#  GRACEFUL SHUTDOWN
# ══════════════════════════════════════════════════════════════════════

def _shutdown():
    log.info("Shutting down …")
    _shutdown_flag.set()
    # Pause all active downloads in aria2
    with _active_gids_lock:
        for gid in list(active_gids.values()):
            _rpc("aria2.forcePause", [gid])
    # Save final state
    _save_tasks()
    # Clean up temp config
    if _aria2_conf_path and os.path.exists(_aria2_conf_path):
        try:
            os.unlink(_aria2_conf_path)
        except OSError:
            pass
    log.info("Shutdown complete.")


atexit.register(_shutdown)
signal.signal(signal.SIGTERM, lambda sig, frame: sys.exit(0))
signal.signal(signal.SIGINT, lambda sig, frame: sys.exit(0))


# ══════════════════════════════════════════════════════════════════════
#  HTML TEMPLATE  (same UI, JS double-fetch fix applied)
# ══════════════════════════════════════════════════════════════════════

HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>Aria2 Scheduler</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'><rect width='16' height='16' rx='3' fill='%230b0f14'/><text x='8' y='13' text-anchor='middle' font-size='12' fill='%234aa8ff'>⬇</text></svg>">
    <style>
        :root {
            color-scheme: dark;
            --bg:#0d1117; --surface:#161b22; --surface-2:#1c222b; --surface-3:#262d38;
            --border:#30363d; --border-strong:#46505c;
            --text:#e6edf3; --muted:#9aa7b4; --faint:#6e7b8a;
            --accent:#58a6ff; --accent-hover:#79b8ff; --accent-text:#0b1622; --accent-tint:rgba(88,166,255,.12);
            --success:#3fb950; --success-tint:rgba(63,185,80,.12);
            --success-btn-tint:rgba(63,185,80,.2); --success-btn:#238636; --success-btn-hover:#2ea043;
            --warn:#d29922; --warn-tint:rgba(210,153,34,.12);
            --danger:#f85149; --danger-tint:rgba(248,81,73,.12);
            --violet:#a371f7; --violet-tint:rgba(163,113,247,.12);
            --cyan:#39c5cf; --cyan-tint:rgba(57,197,207,.12);
            --radius:6px; --radius-sm:4px;
            --font-ui:system-ui,-apple-system,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;
            --font-mono:ui-monospace,SFMono-Regular,"JetBrains Mono","Cascadia Code",Consolas,monospace;
            --xs:.76rem; --sm:.84rem; --base:.92rem; --lg:1.06rem;
        }
        @media (prefers-color-scheme: light) {
            :root {
                color-scheme: light;
                --bg:#f6f8fa; --surface:#ffffff; --surface-2:#f1f4f8; --surface-3:#e6ebf1;
                --border:#d0d7de; --border-strong:#b6c0cc;
                --text:#1f2328; --muted:#57606a; --faint:#7d8590;
                --accent:#0969da; --accent-hover:#0550ae; --accent-text:#ffffff; --accent-tint:rgba(9,105,218,.09);
                --success:#1a7f37; --success-tint:rgba(26,127,55,.09);
                --success-btn-tint:rgba(26,127,55,.15); --success-btn:#1f883d; --success-btn-hover:#1a7f37;
                --warn:#9a6700; --warn-tint:rgba(154,103,0,.09);
                --danger:#cf222e; --danger-tint:rgba(207,34,46,.09);
                --violet:#8250df; --violet-tint:rgba(130,80,223,.09);
                --cyan:#1a7f87; --cyan-tint:rgba(26,127,135,.09);
            }
        }

        * { box-sizing:border-box; }
        [hidden] { display:none !important; }
        html { scroll-behavior:smooth; }
        body {
            font-family:var(--font-ui); font-size:var(--base); line-height:1.5;
            color:var(--text); background:var(--bg);
            max-width:860px; margin:26px auto; padding:0 20px 64px;
            -webkit-font-smoothing:antialiased;
        }
        button, input, textarea { font:inherit; }
        button { cursor:pointer; }
        :focus-visible { outline:2px solid var(--accent); outline-offset:2px; }
        .mono { font-family:var(--font-mono); font-variant-numeric:tabular-nums; }
        .sr-only {
            position:absolute; width:1px; height:1px; padding:0; margin:-1px;
            overflow:hidden; clip:rect(0,0,0,0); white-space:nowrap; border:0;
        }

        /* ── Cards ─────────────────────────────────────── */
        .card {
            background:var(--surface); border:1px solid var(--border);
            border-radius:var(--radius); padding:22px 24px; margin-bottom:18px;
        }
        .card.editing { border-color:var(--violet); }
        .card-head { margin-bottom:20px; padding-right:44px; }
        .card-head h2 {
            margin:0; font-size:var(--lg); font-weight:650; color:var(--text);
        }
        .card-head h2::before {
            content:""; display:inline-block; width:7px; height:7px; border-radius:50%;
            background:var(--accent); margin-right:9px; vertical-align:2px;
        }
        .card.editing .card-head h2::before { background:var(--violet); }
        .card-head p { margin:3px 0 0; font-size:var(--sm); color:var(--muted); }

        .icon-btn {
            display:inline-flex; align-items:center; justify-content:center;
            width:34px; height:34px; padding:0; border-radius:var(--radius-sm);
            background:var(--surface-2); border:1px solid var(--border); color:var(--muted);
            font-size:1rem; line-height:1;
        }
        .icon-btn:hover { color:var(--accent); border-color:var(--accent); }
        #settings-btn { position:absolute; top:18px; right:20px; }
        .card--form { position:relative; }

        /* ── Form fields ────────────────────────────────── */
        .field { margin-bottom:16px; }
        .field > label {
            display:block; margin-bottom:7px; font-size:var(--sm); font-weight:600;
            color:var(--muted);
        }
        .label-hint { font-weight:400; color:var(--faint); font-size:var(--xs); margin-left:6px; }
        input, textarea {
            width:100%; background:var(--bg); border:1px solid var(--border);
            color:var(--text); padding:11px 12px; border-radius:var(--radius-sm);
            font-family:var(--font-mono); font-size:var(--sm);
        }
        input::placeholder, textarea::placeholder { color:var(--faint); }
        input:focus, textarea:focus {
            outline:none; border-color:var(--accent);
        }
        textarea { resize:vertical; min-height:124px; line-height:1.65; }
        input[type="time"] { font-family:var(--font-mono); }

        /* Numbered link editor: gutter + roomy one-line-per-URL rows */
        .editor {
            display:flex; border:1px solid var(--border);
            border-radius:var(--radius-sm); background:var(--bg); overflow:hidden;
        }
        .editor:focus-within { border-color:var(--accent); }
        .editor-gutter {
            flex:0 0 26px; padding:11px 5px; background:var(--surface-2);
            border-right:1px solid var(--border); color:var(--faint);
            font-family:var(--font-mono); font-size:var(--sm);
            line-height:24px; text-align:right; overflow:hidden; user-select:none;
        }
        .editor-gutter span { display:block; }
        #urls {
            flex:1; min-width:0; border:0; border-radius:0; padding:11px 12px;
            background:var(--bg); line-height:24px;
            white-space:pre; overflow-x:auto; resize:vertical;
        }
        #urls:focus { outline:none; }

        .path-input-group {
            display:flex; align-items:stretch; position:relative;
            border:1px solid var(--border); border-radius:var(--radius-sm); background:var(--bg);
        }
        .path-input-group:focus-within { border-color:var(--accent); }
        .path-input-group input { flex:1; min-width:0; border:0; background:transparent; }
        .path-input-group .inline-btn {
            flex-shrink:0; border:0; border-left:1px solid var(--border);
            background:transparent; color:var(--muted); padding:0 12px;
            font-size:var(--xs); white-space:nowrap; user-select:none;
        }
        .path-input-group .inline-btn:hover { color:var(--accent); background:var(--surface-2); }
        .path-input-group .btn-reuse { color:var(--accent); font-weight:600; }
        .path-input-group .btn-reuse:active { background:var(--surface-3); }

        .path-history-drop {
            display:none; position:absolute; top:calc(100% + 4px); right:0;
            background:var(--surface); border:1px solid var(--border-strong);
            border-radius:var(--radius-sm); z-index:20; min-width:280px; max-width:100%;
            max-height:220px; overflow-y:auto;
        }
        .path-history-drop.show { display:block; }
        .path-history-item {
            padding:9px 12px; cursor:pointer; font-family:var(--font-mono); font-size:var(--xs);
            color:var(--muted); border-bottom:1px solid var(--border); word-break:break-all;
        }
        .path-history-item:hover { color:var(--accent); background:var(--surface-2); }
        .path-history-clear {
            padding:8px 12px; cursor:pointer; font-size:var(--xs); color:var(--danger);
            text-align:center; user-select:none;
        }
        .path-history-clear:hover { background:var(--danger-tint); }

        .path-status {
            display:none; align-items:center; flex-wrap:wrap; gap:8px;
            margin-top:7px; font-size:var(--xs); color:var(--muted);
        }
        .path-status.show { display:flex; }
        .path-status strong { color:var(--warn); font-weight:600; }
        .path-status strong::before { content:"⚠ "; }
        .btn-create {
            border:1px solid var(--border-strong); background:var(--surface-2);
            color:var(--accent); padding:2px 8px; border-radius:var(--radius-sm);
            font-size:var(--xs); font-weight:600; line-height:1.5; white-space:nowrap;
        }
        .btn-create:hover { border-color:var(--accent); background:var(--accent-tint); }

        .suggestions { display:flex; flex-wrap:wrap; gap:6px; margin-top:9px; min-height:8px; }
        .sugg-item {
            background:var(--surface-2); padding:4px 10px; border-radius:20px;
            cursor:pointer; font-family:var(--font-mono); font-size:var(--xs);
            border:1px solid var(--border); user-select:none;
        }
        .sugg-item:hover { border-color:var(--accent); color:var(--accent); }

        .field-foot { display:flex; justify-content:space-between; gap:12px; align-items:center; margin-top:7px; }
        .url-count { display:flex; gap:12px; font-size:var(--xs); }
        .url-count .total { color:var(--success); }
        .url-count .bad { color:var(--danger); }
        .kbd-hint { color:var(--faint); font-size:var(--xs); white-space:nowrap; }
        kbd {
            font-family:var(--font-mono); font-size:.95em; padding:1px 5px;
            border:1px solid var(--border-strong); border-radius:3px; background:var(--surface-2);
        }

        .schedule-row {
            display:grid; grid-template-columns:minmax(150px,190px) 1fr;
            gap:14px; align-items:end; margin-top:4px;
        }
        .schedule-row .field { margin-bottom:0; }
        .actions { display:flex; gap:10px; justify-content:flex-end; }
        .actions .btn { flex:1; min-height:44px; }

        .btn {
            display:inline-flex; align-items:center; justify-content:center; gap:6px;
            border:1px solid transparent; padding:7px 11px; min-height:34px;
            border-radius:var(--radius-sm); font-size:var(--sm); font-weight:600;
            white-space:nowrap;
        }
        .btn .ico { font-size:1em; line-height:1; }
        .btn--primary { background:var(--accent); color:var(--accent-text); }
        .btn--primary:hover { background:var(--accent-hover); }
        .btn--accent { background:var(--accent-tint); color:var(--accent); border-color:var(--accent); }
        .btn--accent:hover { background:var(--accent); color:var(--accent-text); }
        .btn--start { background:var(--success-btn-tint); color:var(--success); border-color:var(--success); }
        .btn--start:hover { background:var(--success-btn); color:#fff; border-color:var(--success-btn); }
        .btn--edit { background:var(--violet-tint); color:var(--violet); border-color:var(--violet); }
        .btn--edit:hover { background:var(--violet); color:#fff; }
        .btn--retry { background:var(--cyan-tint); color:var(--cyan); border-color:var(--cyan); }
        .btn--retry:hover { background:var(--cyan); color:var(--accent-text); }
        .btn--retry-outline { background:transparent; color:var(--cyan); border-color:var(--cyan); }
        .btn--retry-outline:hover { background:var(--cyan-tint); }
        .btn--ghost { background:var(--surface-2); color:var(--muted); border-color:var(--border-strong); }
        .btn--ghost:hover { color:var(--text); border-color:var(--muted); }
        .btn--warn { background:var(--warn-tint); color:var(--warn); border-color:var(--warn); }
        .btn--warn:hover { background:var(--warn); color:var(--bg); }
        .btn--danger { background:var(--danger-tint); color:var(--danger); border-color:var(--danger); }
        .btn--danger:hover { background:var(--danger); color:#fff; }
        .btn.is-armed { background:var(--danger); color:#fff; }

        /* ── Jobs ───────────────────────────────────────── */
        #job-list { display:flex; flex-direction:column; gap:11px; }
        .job-card {
            background:var(--surface); border:1px solid var(--border);
            border-left:3px solid var(--accent); border-radius:var(--radius);
            padding:15px 17px;
        }
        .job-card.status-running { border-left-color:var(--success); }
        .job-card.status-paused, .job-card.status-dl_paused { border-left-color:var(--warn); }
        .job-card.status-failed { border-left-color:var(--danger); }
        .job-card.status-waiting, .job-card.status-queued { border-left-color:var(--faint); }
        .job-card.being-edited { border-left-color:var(--accent); }
        .job-top {
            display:flex; justify-content:space-between; align-items:center;
            flex-wrap:wrap; gap:10px;
        }
        .job-heading { display:flex; align-items:center; flex-wrap:wrap; gap:9px; flex:1 1 auto; min-width:0; }
        .job-time { font-size:var(--sm); font-weight:700; color:var(--accent); }
        .job-folder {
            min-width:0; max-width:100%; font-size:var(--xs); color:var(--muted);
            overflow:hidden; text-overflow:ellipsis; white-space:nowrap;
        }
        .job-actions { display:flex; flex:0 0 auto; gap:6px; flex-wrap:nowrap; margin-left:auto; }
        .job-actions .btn { padding:5px 9px; min-height:32px; font-size:var(--xs); }
        .job-sub { margin-top:7px; font-size:var(--xs); color:var(--muted); }
        .job-sub .dot { color:var(--faint); margin:0 5px; }
        .job-sub .eta { color:var(--accent); font-weight:600; }

        .badge {
            display:inline-flex; align-items:center; gap:5px;
            padding:2px 9px; border-radius:20px; font-size:var(--xs); font-weight:700;
            border:1px solid var(--border-strong); color:var(--muted); background:var(--surface-2);
            text-transform:capitalize;
        }
        .badge::before { content:""; width:6px; height:6px; border-radius:50%; background:currentColor; }
        .badge[data-status="running"] { color:var(--success); background:var(--success-tint); border-color:var(--success); }
        .badge[data-status="queued"] { color:var(--accent); background:var(--accent-tint); border-color:var(--accent); }
        .badge[data-status="paused"], .badge[data-status="dl_paused"] { color:var(--warn); background:var(--warn-tint); border-color:var(--warn); }
        .badge[data-status="editing"] { color:var(--violet); background:var(--violet-tint); border-color:var(--violet); }
        .badge[data-status="failed"] { color:var(--danger); background:var(--danger-tint); border-color:var(--danger); }
        .badge[data-status="done"] { color:var(--success); background:var(--success-tint); border-color:var(--success); }

        .job-progress { margin-top:11px; padding-top:11px; border-top:1px solid var(--border); }
        .current-file {
            display:flex; gap:7px; align-items:baseline; font-size:var(--xs);
            color:var(--success); word-break:break-all; margin-bottom:8px;
        }
        .current-file.is-paused { color:var(--warn); }
        .current-file .file-ico { flex-shrink:0; }
        .progress-track { height:8px; border-radius:20px; overflow:hidden; background:var(--surface-3); }
        .progress-fill {
            height:100%; background:var(--success);
            transition:width .25s linear;
        }
        .progress-fill.is-paused { background:var(--warn); }
        .progress-meta {
            display:flex; justify-content:space-between; gap:10px;
            margin-top:7px; font-size:var(--xs); color:var(--muted);
        }
        .progress-meta .speed { color:var(--text); font-weight:600; }

        .failed-box {
            margin-top:10px; padding:8px 10px; border-radius:var(--radius-sm);
            background:var(--danger-tint); border:1px solid var(--danger);
        }
        .failed-box summary {
            cursor:pointer; list-style:none; font-size:var(--xs); font-weight:700;
            color:var(--danger); user-select:none;
        }
        .failed-box summary::-webkit-details-marker { display:none; }
        .failed-box summary::before { content:"▸ "; }
        .failed-box[open] summary::before { content:"▾ "; }
        .failed-list { list-style:none; margin:9px 0 0; padding:0; display:flex; flex-direction:column; gap:6px; }
        .failed-list li { display:flex; align-items:center; gap:8px; font-size:var(--xs); }
        .failed-list .url { flex:1; min-width:0; word-break:break-all; color:var(--muted); }
        .mini-btn {
            flex-shrink:0; min-height:28px; padding:3px 9px; border-radius:var(--radius-sm);
            background:var(--cyan-tint); border:1px solid var(--cyan);
            color:var(--cyan); font-size:var(--xs); font-weight:600;
        }
        .mini-btn:hover { background:var(--cyan); color:var(--accent-text); }

        .empty { text-align:center; padding:34px 18px; color:var(--muted); }
        .empty .empty-ico {
            display:inline-flex; align-items:center; justify-content:center;
            width:46px; height:46px; border-radius:var(--radius); margin-bottom:12px;
            background:var(--accent-tint); border:1px solid var(--accent); color:var(--accent);
            font-size:1.4rem;
        }
        .empty p { margin:0 0 3px; font-weight:650; color:var(--text); }
        .empty span { font-size:var(--sm); color:var(--muted); }

        /* ── Modal ──────────────────────────────────────── */
        .modal-overlay {
            position:fixed; inset:0; z-index:10000; display:none;
            background:rgba(0,0,0,.55);
        }
        .modal-overlay.show { display:flex; align-items:center; justify-content:center; }
        .modal {
            background:var(--surface); border:1px solid var(--border-strong);
            border-radius:var(--radius); padding:22px 24px; width:440px; max-width:92vw;
            max-height:calc(100vh - 40px); overflow-y:auto;
        }
        .modal-head {
            display:flex; align-items:center; justify-content:space-between;
            gap:12px; margin-bottom:18px;
        }
        .modal-head h2 { margin:0; font-size:var(--lg); font-weight:650; }
        .field-grid { display:grid; grid-template-columns:1fr 1fr; gap:13px; }
        select {
            width:100%; background:var(--bg); border:1px solid var(--border);
            color:var(--text); padding:11px 12px; border-radius:var(--radius-sm);
            font-family:var(--font-ui); font-size:var(--sm);
        }
        .advanced { margin-top:14px; padding-top:13px; border-top:1px solid var(--border); }
        .advanced summary {
            cursor:pointer; font-size:var(--sm); font-weight:600;
            color:var(--muted); user-select:none;
        }
        .advanced summary:hover { color:var(--accent); }
        .advanced[open] .field-grid { margin-top:13px; }
        .check-field {
            display:flex; align-items:center; gap:9px; min-height:38px;
            font-size:var(--sm); color:var(--muted); cursor:pointer; user-select:none;
        }
        .check-field input { width:auto; flex:0 0 auto; margin:0; accent-color:var(--accent); }
        .help {
            margin:12px 0 0; padding:10px 12px; border-radius:var(--radius-sm);
            background:var(--surface-2); border:1px solid var(--border);
            font-size:var(--xs); color:var(--muted); line-height:1.6;
        }

        /* ── Toasts ─────────────────────────────────────── */
        #toast-container {
            position:fixed; bottom:18px; right:18px; z-index:10001;
            display:flex; flex-direction:column; gap:9px; max-width:390px;
        }
        .toast {
            display:flex; gap:10px; align-items:flex-start;
            background:var(--surface); border:1px solid var(--border-strong);
            border-left:3px solid var(--accent); border-radius:var(--radius-sm);
            padding:11px 13px; font-size:var(--sm);
        }
        .toast.error { border-left-color:var(--danger); }
        .toast.warning { border-left-color:var(--warn); }
        .toast.success { border-left-color:var(--success); }
        .toast .toast-icon { flex-shrink:0; font-weight:700; color:var(--accent); }
        .toast.error .toast-icon { color:var(--danger); }
        .toast.warning .toast-icon { color:var(--warn); }
        .toast.success .toast-icon { color:var(--success); }
        .toast .toast-msg { flex:1; word-break:break-word; }
        .toast .toast-close {
            border:0; background:transparent; color:var(--faint);
            font-size:1rem; line-height:1; padding:0 2px;
        }
        .toast .toast-close:hover { color:var(--text); }

        /* ── Responsive ─────────────────────────────────── */
        @media (max-width:620px) {
            body { margin:0; padding:14px 13px 50px; }
            .card { padding:18px 16px; border-radius:var(--radius-sm); }
            #settings-btn { top:15px; right:16px; }
            .card-head { padding-right:40px; }
            .schedule-row { grid-template-columns:1fr; gap:12px; }
            .actions .btn { min-height:42px; }
            .job-heading { width:100%; }
            .job-actions { width:100%; flex-wrap:wrap; margin-left:0; }
            .job-actions .btn { flex:1; min-height:38px; padding:7px 9px; }
            .field-foot { flex-direction:column; align-items:flex-start; gap:4px; }
            .path-history-drop { min-width:0; left:0; right:0; }
            .field-grid { grid-template-columns:1fr; }
            #toast-container { left:12px; right:12px; bottom:12px; max-width:none; }
        }
    </style>
</head>
<body>

<div id="toast-container" role="status" aria-live="polite" aria-atomic="false"></div>

<section class="card card--form" id="form-card" aria-labelledby="form-title">
    <button id="settings-btn" class="icon-btn" type="button" title="Download settings" aria-label="Open download settings" onclick="openSettings()">⚙</button>
    <div class="card-head">
        <h2 id="form-title">New task</h2>
    </div>

    <div class="field">
        <label for="dest_folder">Destination folder<span class="label-hint">Tab to complete</span></label>
        <div class="path-input-group">
            <input type="text" id="dest_folder" autocomplete="off" spellcheck="false" placeholder="/home/user/Downloads" aria-describedby="path-status">
            <button type="button" class="inline-btn" id="btn-history" title="Recent paths" aria-label="Recent paths" onclick="togglePathHistory()">▾</button>
            <button type="button" class="inline-btn btn-reuse" id="btn-reuse" title="Reload last used path" aria-label="Reload last used path" onclick="reuseLastPath()" hidden>↻ Reuse</button>
            <div class="path-history-drop" id="path-history-drop"></div>
        </div>
        <div id="path-status" class="path-status" role="status">
            <strong>Path doesn't exist.</strong>
            <button type="button" class="btn-create" onclick="createPath()">Create folder</button>
        </div>
        <div id="suggestion-box" class="suggestions" aria-label="Path suggestions"></div>
    </div>

    <div class="field">
        <label for="urls">Download links<span class="label-hint">one per line</span></label>
        <div class="editor" id="url-editor">
            <div class="editor-gutter" id="url-gutter" aria-hidden="true"></div>
            <textarea id="urls" rows="5" wrap="off" spellcheck="false" placeholder="https://example.com/file1&#10;https://example.com/file2" aria-describedby="url-count"></textarea>
        </div>
        <div class="field-foot">
            <div class="url-count" id="url-count" aria-live="polite"></div>
            <span class="kbd-hint"><kbd>Ctrl</kbd>+<kbd>Enter</kbd> to start</span>
        </div>
    </div>

    <div class="schedule-row">
        <div class="field">
            <label for="start_time">Start at</label>
            <input type="time" id="start_time">
        </div>
        <div class="actions" id="btns-normal">
            <button type="button" class="btn btn--accent" onclick="submitTask('schedule')"><span class="ico">◷</span> Schedule</button>
            <button type="button" class="btn btn--start" onclick="submitTask('now')"><span class="ico">↓</span> Start now</button>
        </div>
        <div class="actions" id="btns-edit" hidden>
            <button type="button" class="btn btn--ghost" onclick="cancelEdit()">Cancel</button>
            <button type="button" class="btn btn--primary" onclick="saveEdit()"><span class="ico">✓</span> Save changes</button>
        </div>
    </div>
</section>

<div id="job-list" aria-label="Scheduled and active downloads">
    <div class="empty" id="empty-state"><div class="empty-ico" aria-hidden="true">↓</div><p>No tasks yet</p><span>Paste a few links above to get started.</span></div>
</div>

<div id="settings-modal" class="modal-overlay">
    <div class="modal" role="dialog" aria-modal="true" aria-labelledby="settings-title">
        <div class="modal-head">
            <h2 id="settings-title">Download settings</h2>
            <button type="button" class="icon-btn" aria-label="Close settings" title="Close" onclick="closeSettings()">✕</button>
        </div>
        <div class="field-grid">
            <div class="field">
                <label for="set_conns">Connections / server</label>
                <input type="number" id="set_conns" data-setting="connections_per_server" data-type="int" min="1" max="64" step="1">
            </div>
            <div class="field">
                <label for="set_mss">Min segment size</label>
                <input type="text" id="set_mss" data-setting="min_split_size" placeholder="8M" spellcheck="false">
            </div>
            <div class="field">
                <label for="set_alloc">File allocation</label>
                <select id="set_alloc" data-setting="file_allocation">
                    <option value="none">None — no preallocation</option>
                    <option value="falloc">Fast (falloc)</option>
                    <option value="prealloc">Preallocate</option>
                    <option value="trunc">Truncate</option>
                </select>
            </div>
            <div class="field">
                <label for="set_limit">Speed limit</label>
                <input type="text" id="set_limit" data-setting="max_download_limit" placeholder="0 = unlimited" spellcheck="false">
            </div>
            <div class="field">
                <label for="set_tries">Max tries</label>
                <input type="number" id="set_tries" data-setting="max_tries" data-type="int" min="0" max="64" step="1">
            </div>
            <div class="field">
                <label for="set_retry_wait">Retry wait (s)</label>
                <input type="number" id="set_retry_wait" data-setting="retry_wait" data-type="int" min="0" max="600" step="1">
            </div>
        </div>
        <details class="advanced">
            <summary>Advanced options</summary>
            <div class="field-grid">
                <label class="check-field"><input type="checkbox" data-setting="check_certificate"> Verify TLS certificates</label>
                <label class="check-field"><input type="checkbox" data-setting="remote_time"> Preserve remote timestamps</label>
                <div class="field">
                    <label for="set_ctimeout">Connect timeout (s)</label>
                    <input type="number" id="set_ctimeout" data-setting="connect_timeout" data-type="int" min="1" max="600" step="1">
                </div>
                <div class="field">
                    <label for="set_timeout">Timeout (s)</label>
                    <input type="number" id="set_timeout" data-setting="timeout" data-type="int" min="1" max="600" step="1">
                </div>
                <div class="field">
                    <label for="set_ua">User-Agent</label>
                    <input type="text" id="set_ua" data-setting="user_agent" placeholder="Default" spellcheck="false">
                </div>
                <div class="field">
                    <label for="set_ref">Referer</label>
                    <input type="text" id="set_ref" data-setting="referer" placeholder="None" spellcheck="false">
                </div>
            </div>
        </details>
        <p class="help">Connections and segment size control parallel ranges. File allocation <span class="mono">none</span> avoids preallocation delays on network storage. Speed limit accepts <span class="mono">0</span> (unlimited) or values like <span class="mono">10M</span>. Changes apply to new downloads.</p>
        <div class="actions" style="margin-top:18px;">
            <button type="button" class="btn btn--ghost" onclick="closeSettings()">Cancel</button>
            <button type="button" class="btn btn--primary" onclick="saveSettings()">Save settings</button>
        </div>
    </div>
</div>

<script>
    let suggestions  = [];
    let editingJobId = null;
    let pollTimer    = null;
    let pollBusy     = false;
    let lastNetToast = 0;
    let jobViews     = new Map();
    let jobOrder     = null;
    let renderQueued = false;
    let pendingJobs  = null;
    let pathTimer    = null;
    let pathFetch    = null;
    let lastJobs     = [];
    let lastFocus    = null;

    const destInput     = document.getElementById('dest_folder');
    const suggBox       = document.getElementById('suggestion-box');
    const pathStatus    = document.getElementById('path-status');
    const formCard      = document.getElementById('form-card');
    const formTitle     = document.getElementById('form-title');
    const btnsNormal    = document.getElementById('btns-normal');
    const btnsEdit      = document.getElementById('btns-edit');
    const urlsTextarea  = document.getElementById('urls');
    const urlGutter     = document.getElementById('url-gutter');
    const urlCountEl    = document.getElementById('url-count');
    const pathHistDrop  = document.getElementById('path-history-drop');
    const settingsModal = document.getElementById('settings-modal');

    const BASE_TITLE = 'Aria2 Scheduler';

    const HTML_ESCAPES = {'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'};
    function escapeHtml(str) {
        return String(str ?? '').replace(/[&<>"']/g, c => HTML_ESCAPES[c]);
    }

    // ── Path history ──────────────────────────────────────

    function getPathHistory() {
        try { return JSON.parse(localStorage.getItem('pathHistory') || '[]'); }
        catch { return []; }
    }

    function savePathHistory(path) {
        let hist = getPathHistory().filter(p => p !== path);
        hist.unshift(path);
        if (hist.length > 8) hist = hist.slice(0, 8);
        localStorage.setItem('pathHistory', JSON.stringify(hist));
    }

    function recordPathUsed(path) {
        lastUsedPath = path;
        localStorage.setItem('lastUsedPath', lastUsedPath);
        savePathHistory(path);
        updateReuseButton();
    }

    let lastUsedPath = localStorage.getItem('lastUsedPath') || '';

    function updateReuseButton() {
        const btn = document.getElementById('btn-reuse');
        const histBtn = document.getElementById('btn-history');
        btn.hidden = !lastUsedPath;
        btn.title = lastUsedPath ? 'Reuse: ' + lastUsedPath : '';
        histBtn.style.borderRadius = lastUsedPath ? '0' : 'var(--radius-sm)';
    }

    function reuseLastPath() {
        if (!lastUsedPath) return;
        destInput.value = lastUsedPath;
        destInput.focus();
        destInput.setSelectionRange(destInput.value.length, destInput.value.length);
        checkPath(lastUsedPath);
    }

    function togglePathHistory() {
        const hist = getPathHistory();
        if (!hist.length) return;
        if (pathHistDrop.classList.contains('show')) {
            pathHistDrop.classList.remove('show');
            return;
        }
        pathHistDrop.innerHTML =
            hist.map(p => '<div class="path-history-item" data-path="' + escapeHtml(p) + '">' + escapeHtml(p) + '</div>').join('') +
            '<div class="path-history-clear">Clear history</div>';
        pathHistDrop.classList.add('show');
    }

    document.addEventListener('click', e => {
        if (!e.target.closest('.path-input-group')) pathHistDrop.classList.remove('show');
    });

    pathHistDrop.addEventListener('click', e => {
        const item = e.target.closest('.path-history-item');
        if (item && item.dataset.path) {
            destInput.value = item.dataset.path;
            pathHistDrop.classList.remove('show');
            checkPath(item.dataset.path);
        }
        if (e.target.closest('.path-history-clear')) {
            localStorage.removeItem('pathHistory');
            pathHistDrop.classList.remove('show');
        }
    });

    updateReuseButton();

    // ── Path autocomplete ────────────────────────────────

    destInput.addEventListener('keydown', e => {
        if (e.key !== 'Tab') return;
        e.preventDefault();
        if (suggestions.length === 1) {
            destInput.value = suggestions[0];
            checkPath(destInput.value);
        } else if (suggestions.length > 1) {
            destInput.value = suggestions.reduce((a, b) => {
                let i = 0;
                while (a[i] && a[i] === b[i]) i++;
                return a.slice(0, i);
            });
        }
    });

    destInput.addEventListener('input', e => checkPath(e.target.value));

    function checkPath(val) {
        clearTimeout(pathTimer);
        if (pathFetch) pathFetch.abort();
        if (!val) {
            suggBox.innerHTML = '';
            pathStatus.classList.remove('show');
            return;
        }
        // Debounce + abort: typing stays snappy and stale results can't
        // overwrite the suggestions for the current value.
        pathTimer = setTimeout(async () => {
            pathFetch = new AbortController();
            try {
                const res  = await fetch('/autocomplete?term=' + encodeURIComponent(val), {signal: pathFetch.signal});
                const data = await res.json();
                if (destInput.value !== val) return;
                suggestions = data.paths;

                suggBox.innerHTML = suggestions.map(s => {
                    const label = s.split('/').filter(x => x).pop() || s;
                    return '<button type="button" class="sugg-item" data-path="' + escapeHtml(s) + '">' + escapeHtml(label) + '</button>';
                }).join('');

                suggBox.querySelectorAll('.sugg-item').forEach(el => {
                    el.addEventListener('mousedown', e => {
                        e.preventDefault();
                        destInput.value = el.dataset.path;
                        checkPath(el.dataset.path);
                    });
                });

                pathStatus.classList.toggle('show', !data.exists && suggestions.length === 0);
            } catch (e) {
                if (e.name !== 'AbortError') pathStatus.classList.remove('show');
            }
        }, 130);
    }

    // ── Toasts ───────────────────────────────────────────

    function showToast(msg, type) {
        type = type || 'error';
        const icons = {error:'✕', warning:'⚠', success:'✓'};
        const container = document.getElementById('toast-container');
        const toast = document.createElement('div');
        toast.className = 'toast ' + type;
        toast.innerHTML =
            '<span class="toast-icon" aria-hidden="true">' + icons[type] + '</span>' +
            '<span class="toast-msg">' + escapeHtml(msg) + '</span>' +
            '<button type="button" class="toast-close" aria-label="Dismiss notification">✕</button>';
        toast.querySelector('.toast-close').addEventListener('click', () => toast.remove());
        container.appendChild(toast);
        setTimeout(() => toast.remove(), 3900);
    }

    async function safeFetch(url, opts) {
        try {
            const res = await fetch(url, opts);
            if (!res.ok) {
                const data = await res.json().catch(() => ({}));
                throw new Error(data.message || 'Server error (' + res.status + ')');
            }
            return res;
        } catch (e) {
            // Polling can fail repeatedly when the server restarts; don't
            // stack a toast on every attempt.
            const now = Date.now();
            if (now - lastNetToast > 20000) {
                lastNetToast = now;
                if (e.name === 'TypeError' && e.message.includes('fetch')) {
                    showToast('Cannot reach server. Is the app running?', 'error');
                } else {
                    showToast(e.message, 'error');
                }
            }
            throw e;
        }
    }

    async function createPath() {
        await fetch('/create_path', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({path: destInput.value})
        });
        checkPath(destInput.value);
    }

    // ── URL parsing ──────────────────────────────────────

    function getUrls() {
        return urlsTextarea.value.split(/[\n\r]+/).map(u => u.trim()).filter(u => u);
    }

    const URL_RE = /^https?:\/\//i;
    function isValidUrl(u) { return URL_RE.test(u); }

    function updateUrlCount() {
        const urls = getUrls();
        const total = urls.length;
        const invalid = urls.filter(u => !isValidUrl(u)).length;
        if (!total) { urlCountEl.innerHTML = ''; return; }
        let html = '<span class="total">' + total + ' link' + (total !== 1 ? 's' : '') + '</span>';
        if (invalid) html += '<span class="bad">' + invalid + ' invalid</span>';
        urlCountEl.innerHTML = html;
    }

    function updateGutter() {
        const count = Math.max(1, urlsTextarea.value.split('\n').length);
        let html = '';
        for (let i = 1; i <= count; i++) html += '<span>' + i + '</span>';
        urlGutter.innerHTML = html;
        urlGutter.style.flexBasis = Math.max(26, String(count).length * 9 + 10) + 'px';
        urlGutter.scrollTop = urlsTextarea.scrollTop;
    }

    urlsTextarea.addEventListener('input', () => { updateGutter(); updateUrlCount(); });
    urlsTextarea.addEventListener('scroll', () => { urlGutter.scrollTop = urlsTextarea.scrollTop; });
    urlsTextarea.addEventListener('paste', () => setTimeout(() => {
        // Browsers scroll long lines to reveal the caret after a paste —
        // snap back so the start of the links stays visible next to the
        // line numbers.
        urlsTextarea.scrollLeft = 0;
        updateGutter();
        updateUrlCount();
    }, 50));
    urlsTextarea.addEventListener('keydown', e => {
        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
            e.preventDefault();
            submitTask(document.getElementById('start_time').value ? 'schedule' : 'now');
        }
    });

    // ── Edit mode ────────────────────────────────────────

    function enterEditMode(job) {
        editingJobId = job.id;
        destInput.value = job.folder;
        urlsTextarea.value = job.urls.join('\n');
        document.getElementById('start_time').value = (job.time === 'NOW' ? '' : job.time);

        formCard.classList.add('editing');
        formTitle.textContent = 'Editing task';
        btnsNormal.hidden = true;
        btnsEdit.hidden = false;
        window.scrollTo({top: 0, behavior: 'smooth'});
        checkPath(job.folder);
        updateGutter();
        updateUrlCount();
        refreshJobs();
    }

    function cancelEdit() {
        editingJobId = null;
        clearForm();
        formCard.classList.remove('editing');
        formTitle.textContent = 'New task';
        btnsNormal.hidden = false;
        btnsEdit.hidden = true;
        refreshJobs();
    }

    function clearForm() {
        destInput.value = '';
        urlsTextarea.value = '';
        document.getElementById('start_time').value = '';
        suggBox.innerHTML = '';
        pathStatus.classList.remove('show');
        urlGutter.innerHTML = '';
        urlCountEl.innerHTML = '';
    }

    async function saveEdit() {
        const folder = destInput.value.trim();
        const urls   = urlsTextarea.value.trim();
        if (!folder) { showToast('Please enter a destination path.', 'warning'); return; }
        if (!urls)   { showToast('Please enter at least one link.', 'warning'); return; }

        await safeFetch('/edit/' + encodeURIComponent(editingJobId), {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({dest_folder: folder, urls: urls, start_time: document.getElementById('start_time').value})
        });
        recordPathUsed(folder);
        cancelEdit();
    }

    // ── Form submission ──────────────────────────────────

    async function submitTask(mode) {
        const folder = destInput.value.trim();
        const urls   = urlsTextarea.value.trim();
        const t      = document.getElementById('start_time').value;
        if (!folder) { showToast('Please enter a destination path.', 'warning'); return; }
        if (!urls)   { showToast('Please enter at least one link.', 'warning'); return; }
        if (mode === 'schedule' && !t) { showToast('Please select a time.', 'warning'); return; }

        await safeFetch('/schedule', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                dest_folder: folder,
                urls:        urls,
                start_time:  t,
                start_now:   mode === 'now'
            })
        });
        recordPathUsed(folder);
        clearForm();
        refreshJobs();
    }

    // ── Time helpers ─────────────────────────────────────

    const BYTE_UNITS = {B:1, KB:1024, MB:1024**2, GB:1024**3, TB:1024**4};

    function parseBytes(text) {
        const m = String(text || '').trim().match(/^([\d.,]+)\s*(B|KB|MB|GB|TB)$/i);
        if (!m) return 0;
        return parseFloat(m[1].replace(/,/g, '')) * BYTE_UNITS[m[2].toUpperCase()];
    }

    function parseSpeed(text) {
        const m = String(text || '').trim().match(/^([\d.,]+)\s*(B|KB|MB|GB|TB)\/s$/i);
        return m ? parseFloat(m[1].replace(/,/g, '')) * BYTE_UNITS[m[2].toUpperCase()] : 0;
    }

    function durationWords(seconds, maxUnits) {
        seconds = Math.max(0, Math.round(seconds));
        const days = Math.floor(seconds / 86400); seconds %= 86400;
        const hours = Math.floor(seconds / 3600); seconds %= 3600;
        const mins = Math.floor(seconds / 60);
        const parts = [];
        if (days) parts.push(days + 'd');
        if (hours) parts.push(hours + 'h');
        if (mins) parts.push(mins + 'm');
        if (!parts.length) parts.push(Math.max(1, seconds) + 's');
        return parts.slice(0, maxUnits || 2).join(' ');
    }

    function etaOf(j) {
        if (j.status !== 'running') return '';
        const speed = parseSpeed(j.speed);
        if (!speed) return '';
        const parts = String(j.size || '').split('/');
        const done  = parseBytes(parts[0]);
        const total = parseBytes(parts[1] || parts[0]);
        let remaining = total > done ? total - done : 0;
        const pct = parseFloat(j.progress);
        if (!remaining && pct > 0 && pct < 100 && total > 0) remaining = total * (100 - pct) / 100;
        if (!remaining) return '';
        return '~' + durationWords(remaining / speed);
    }

    function countdownText(j) {
        if (j.status !== 'waiting' && j.status !== 'queued') return '';
        if (j.sched_paused) return 'Schedule paused';
        if (j.time === 'NOW') return 'Starting shortly';
        const [h, m] = j.time.split(':').map(Number);
        if (!Number.isFinite(h) || !Number.isFinite(m)) return '';
        const now = new Date();
        const target = new Date(now);
        target.setHours(h, m, 0, 0);
        if (target <= now) target.setDate(target.getDate() + 1);
        return 'in ' + durationWords((target - now) / 1000);
    }

    // ── Job list ─────────────────────────────────────────

    const ICONS = {
        start_now:'▶', pause:'⏸', resume:'▶', dl_pause:'⏸', dl_resume:'▶',
        edit:'✎', del:'✕', retry:'↻', retry_failed:'↻'
    };

    function actionBtn(id, action, label, cls) {
        return '<button type="button" class="btn ' + cls + '" ' +
               'aria-label="' + escapeHtml(label + ' (' + id + ')') + '" ' +
               'onclick="control(\'' + escapeHtml(id) + '\',\'' + action + '\')">' +
               '<span class="ico" aria-hidden="true">' + ICONS[action] + '</span>' + escapeHtml(label) + '</button>';
    }

    function jobSignature(j) {
        return [j.time, j.folder, j.status, j.url_total, j.done_count,
                j.sched_paused, j.failed_urls.join('|'), j.id === editingJobId].join('\u0001');
    }

    function jobButtons(j) {
        const isRunning = j.status === 'running';
        const isPaused  = j.status === 'paused' || j.status === 'dl_paused';
        const isFailed  = j.status === 'failed';
        const isWaiting = j.status === 'waiting' || j.status === 'queued';
        const isActive  = isRunning || isPaused;
        if (j.id === editingJobId) return '';

        let btns = '';
        if (isWaiting) {
            btns += actionBtn(j.id, 'start_now', 'Start now', 'btn--start');
            btns += actionBtn(j.id, 'pause', j.sched_paused ? 'Resume schedule' : 'Pause schedule', 'btn--warn');
        }
        if (isRunning) btns += actionBtn(j.id, 'dl_pause', 'Pause', 'btn--warn');
        if (isPaused)  btns += actionBtn(j.id, 'dl_resume', 'Resume', 'btn--start');
        if (!isActive) {
            btns += '<button type="button" class="btn btn--edit" aria-label="Edit task ' + escapeHtml(j.id) + '" ' +
                    'onclick="editJob(\'' + escapeHtml(j.id) + '\')"><span class="ico" aria-hidden="true">✎</span>Edit</button>';
        }
        btns += '<button type="button" class="btn btn--danger" data-armed="0" aria-label="Delete task ' + escapeHtml(j.id) + '" ' +
                'onclick="armDelete(\'' + escapeHtml(j.id) + '\',this)"><span class="ico" aria-hidden="true">✕</span>Delete</button>';
        if (isFailed) {
            btns += actionBtn(j.id, 'retry', 'Retry', 'btn--retry');
            if (j.failed_urls.length && j.done_count > 0) {
                btns += actionBtn(j.id, 'retry_failed', 'Retry failed', 'btn--retry-outline');
            }
        }
        return btns;
    }

    function failedHTML(j) {
        if (j.status !== 'failed' || !j.failed_urls.length) {
            return '<div class="failed-box" data-role="failed" hidden></div>';
        }
        return `<details class="failed-box" data-role="failed">
            <summary>${j.failed_urls.length} failed link${j.failed_urls.length !== 1 ? 's' : ''} — click to expand</summary>
            <ul class="failed-list">
                ${j.failed_urls.map(u =>
                    '<li><span class="url">' + escapeHtml(u) + '</span>' +
                    '<button type="button" class="mini-btn" aria-label="Retry link ' + escapeHtml(u) + '" ' +
                    'onclick="retryUrl(\'' + escapeHtml(j.id) + '\',this.dataset.u)" data-u="' + escapeHtml(u) + '">Retry</button></li>'
                ).join('')}
            </ul>
        </details>`;
    }

    function cardHTML(j) {
        const isRunning  = j.status === 'running';
        const isPaused   = j.status === 'paused' || j.status === 'dl_paused';
        const isFailed   = j.status === 'failed';
        const isActive   = isRunning || isPaused;
        const isEditing  = j.id === editingJobId;
        const showFailed = Boolean(isFailed && j.failed_urls.length);
        const statusLabel = isEditing ? 'editing' : j.status;
        const pct = parseFloat(j.progress) || 0;

        return `
        <article class="job-card status-${escapeHtml(j.status)} ${isEditing ? 'being-edited' : ''}" data-job-id="${escapeHtml(j.id)}">
            <div class="job-top">
                <div class="job-heading">
                    <span class="job-time mono" data-role="time">[${escapeHtml(j.time)}]</span>
                    <span class="badge" data-role="badge" data-status="${escapeHtml(statusLabel)}">${escapeHtml(statusLabel.replace('_', ' '))}</span>
                    <span class="job-folder mono" data-role="folder" title="${escapeHtml(j.folder)}">${escapeHtml(j.folder)}</span>
                </div>
                <div class="job-actions" data-role="actions">${jobButtons(j)}</div>
            </div>
            <div class="job-sub" data-role="header-sub" ${countdownText(j) ? '' : 'hidden'}>
                <span class="eta" data-role="countdown"></span>
            </div>
            <div class="job-progress" data-role="progress-block" ${isActive || showFailed ? '' : 'hidden'}>
                <div class="download-stats" data-role="stats" ${isActive ? '' : 'hidden'}>
                    <div class="current-file mono ${isPaused ? 'is-paused' : ''}" data-role="file">
                        <span class="file-ico" aria-hidden="true">${isPaused ? '⏸' : '↓'}</span>
                        <span>${escapeHtml(j.current_file || j.current_url || 'Waiting for file…')}</span>
                    </div>
                    <div class="progress-track" role="progressbar" aria-label="Download progress"
                         aria-valuemin="0" aria-valuemax="100" aria-valuenow="${pct}" data-role="bar-wrap">
                        <div class="progress-fill ${isPaused ? 'is-paused' : ''}" style="width:${pct}%" data-role="bar"></div>
                    </div>
                    <div class="progress-meta">
                        <span><span data-role="pct">${pct}%</span> <span class="dot">·</span> <span data-role="size">${escapeHtml(j.size || '—')}</span></span>
                        <span class="speed" data-role="speed">${isPaused ? 'Paused' : escapeHtml(j.speed || '0 B/s')}</span>
                    </div>
                    <div class="job-sub" data-role="detail"></div>
                </div>
                ${failedHTML(j)}
            </div>
        </article>`;
    }

    // Index a card's data-role nodes once so polling never runs a selector
    // query per field. Only the rare failed-box replacement needs a re-query.
    function indexRoles(el) {
        const map = new Map();
        el.querySelectorAll('[data-role]').forEach(n => map.set(n.dataset.role, n));
        return map;
    }

    function setText(view, name, text) {
        const node = view.roles.get(name);
        if (node) node.textContent = text;
    }

    function updateJobDynamic(view, j) {
        const q = name => view.roles.get(name);
        const isPaused = j.status === 'paused' || j.status === 'dl_paused';
        const active = j.status === 'running' || isPaused;

        const block = q('progress-block');
        if (block) block.hidden = !(active || (j.status === 'failed' && j.failed_urls.length));
        const stats = q('stats');
        if (stats) stats.hidden = !active;
        const countdown = countdownText(j);
        setText(view, 'countdown', countdown);
        const headerSub = q('header-sub');
        if (headerSub) headerSub.hidden = !countdown;

        if (!active) return;
        const pct = parseFloat(j.progress) || 0;
        const bar = q('bar');
        if (bar) {
            bar.style.width = pct + '%';
            bar.classList.toggle('is-paused', isPaused);
        }
        const wrap = q('bar-wrap');
        if (wrap) wrap.setAttribute('aria-valuenow', String(Math.round(pct)));
        const file = q('file');
        if (file) {
            file.classList.toggle('is-paused', isPaused);
            const ico = file.querySelector('.file-ico');
            if (ico) ico.textContent = isPaused ? '⏸' : '↓';
            file.lastElementChild.textContent = ' ' + (j.current_file || j.current_url || 'Waiting for file…');
        }
        setText(view, 'pct', pct + '%');
        setText(view, 'size', j.size || '—');
        setText(view, 'speed', isPaused ? 'Paused' : (j.speed || '0 B/s'));
        const eta = etaOf(j);
        setText(view, 'detail', (j.done_count || 0) + ' / ' + j.url_total + ' files done' + (eta ? ' · ETA ' + eta : ''));
    }

    // Patch a card in place — never replace the whole node, so focus,
    // scroll position and open <details> survive a refresh.
    function patchJob(view, j) {
        const el = view.el;
        const q = name => view.roles.get(name);
        const sig = jobSignature(j);
        if (sig !== view.sig) {
            const isEditing = j.id === editingJobId;
            const status = isEditing ? 'editing' : j.status;
            el.className = 'job-card status-' + j.status + (isEditing ? ' being-edited' : '');
            setText(view, 'time', '[' + j.time + ']');
            const b = q('badge');
            if (b) { b.dataset.status = status; b.textContent = status.replace('_', ' '); }
            const folder = q('folder');
            if (folder) { folder.textContent = j.folder; folder.title = j.folder; }
            const actions = q('actions');
            if (actions) actions.innerHTML = jobButtons(j);
            const failed = q('failed');
            if (failed) {
                failed.outerHTML = failedHTML(j);
                const fresh = el.querySelector('[data-role="failed"]');
                if (fresh) view.roles.set('failed', fresh);
            }
            view.sig = sig;
        }
        updateJobDynamic(view, j);
    }

    function renderJobsNow(jobs) {
        const list = document.getElementById('job-list');
        document.getElementById('empty-state').hidden = jobs.length > 0;
        const ids = jobs.map(j => j.id).join('|');

        if (ids !== jobOrder) {
            const present = new Set(jobs.map(j => j.id));
            for (const [id, view] of jobViews) {
                if (!present.has(id)) { view.el.remove(); jobViews.delete(id); }
            }
            jobs.forEach((j, index) => {
                let view = jobViews.get(j.id);
                if (!view) {
                    const holder = document.createElement('div');
                    holder.innerHTML = cardHTML(j);
                    const node = holder.firstElementChild;
                    view = {el: node, sig: null, roles: indexRoles(node)};
                    jobViews.set(j.id, view);
                }
                const ref = list.children[index + 1] || null; // empty-state is child 0
                if (view.el !== ref) list.insertBefore(view.el, ref);
            });
            jobOrder = ids;
        }

        jobs.forEach(j => {
            const view = jobViews.get(j.id);
            if (view) patchJob(view, j);
        });
    }

    // Coalesce renders triggered by multiple responses in the same frame.
    function refreshJobsInner(jobs) {
        pendingJobs = jobs;
        if (renderQueued) return;
        renderQueued = true;
        requestAnimationFrame(() => {
            renderQueued = false;
            renderJobsNow(pendingJobs);
        });
        // rAF can be suspended for occluded windows — don't let updates stall.
        setTimeout(() => {
            if (renderQueued) {
                renderQueued = false;
                renderJobsNow(pendingJobs);
            }
        }, 120);
    }

    function disarm(btn) {
        if (!btn.dataset.armed) return;
        delete btn.dataset.armed;
        btn.classList.remove('is-armed');
        if (btn.dataset.prevHTML) btn.innerHTML = btn.dataset.prevHTML;
        delete btn.dataset.prevHTML;
    }

    function disarmAll() {
        document.querySelectorAll('.btn[data-armed]').forEach(disarm);
    }

    function armDelete(id, btn) {
        if (btn.dataset.armed === '1') {
            control(id, 'delete');
            return;
        }
        disarmAll();
        btn.dataset.armed = '1';
        btn.dataset.prevHTML = btn.innerHTML;
        btn.classList.add('is-armed');
        btn.textContent = 'Confirm delete?';
        btn.setAttribute('aria-label', 'Click again to confirm delete of ' + id);
        setTimeout(() => { if (btn.isConnected) disarm(btn); }, 4000);
    }

    function retryUrl(id, url) {
        control(id, 'retry_url', {url: url});
    }

    async function editJob(id) {
        let job = lastJobs.find(j => j.id === id);
        if (!job) {
            try {
                const res = await safeFetch('/jobs');
                job = (await res.json()).find(j => j.id === id);
            } catch { return; }
        }
        if (job) enterEditMode(job);
    }

    async function control(id, action, payload) {
        disarmAll();
        const opts = {method: 'POST'};
        if (payload) {
            opts.headers = {'Content-Type': 'application/json'};
            opts.body = JSON.stringify(payload);
        }
        try {
            await safeFetch('/control/' + action + '/' + encodeURIComponent(id), opts);
        } catch {}
        refreshJobs();
    }

    function updateTabTitle(jobs) {
        let running = 0, paused = 0, failed = 0;
        for (const j of jobs) {
            if (j.status === 'running') running++;
            else if (j.status === 'paused' || j.status === 'dl_paused') paused++;
            else if (j.status === 'failed') failed++;
        }
        let prefix = '';
        if (running)     prefix = '⬇ ';
        else if (paused) prefix = '⏸ ';
        else if (failed) prefix = '✗ ';
        const title = prefix + BASE_TITLE;
        if (document.title !== title) document.title = title;
    }

    // ── Polling (one chain, never overlapping requests) ───

    async function loadJobs() {
        if (pollBusy) return null;
        pollBusy = true;
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 8000);
        try {
            const res = await safeFetch('/jobs', {signal: controller.signal});
            const jobs = await res.json();
            lastJobs = jobs;
            refreshJobsInner(jobs);
            updateTabTitle(jobs);
            return jobs;
        } catch {
            return null;
        } finally {
            clearTimeout(timeout);
            pollBusy = false;
        }
    }

    function scheduleNext(ms) {
        clearTimeout(pollTimer);
        pollTimer = setTimeout(poll, ms);
    }

    function reschedule(jobs) {
        const busy = jobs && jobs.some(j => j.status === 'running' || j.status === 'paused' || j.status === 'dl_paused');
        scheduleNext(document.hidden ? 15000 : busy ? 1500 : 5000);
    }

    function poll() {
        clearTimeout(pollTimer);
        loadJobs().then(jobs => {
            if (jobs === null && pollBusy) scheduleNext(600); // another refresh owns the request
            else reschedule(jobs);
        });
    }

    function refreshJobs() { poll(); }

    document.addEventListener('visibilitychange', () => {
        if (document.hidden) scheduleNext(15000);
        else poll();
    });

    // ── Settings ────────────────────────────────────────

    const focusableSelector = 'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])';

    function openSettings() {
        lastFocus = document.activeElement;
        settingsModal.classList.add('show');
        loadSettings();
        document.getElementById('set_conns').focus();
        document.getElementById('set_conns').select();
    }

    function closeSettings() {
        settingsModal.classList.remove('show');
        if (lastFocus && lastFocus.focus) lastFocus.focus();
    }

    const settingInputs = [...settingsModal.querySelectorAll('[data-setting]')];

    async function loadSettings() {
        try {
            const res = await safeFetch('/settings');
            const s = (await res.json()).settings || {};
            for (const el of settingInputs) {
                const key = el.dataset.setting;
                if (!(key in s)) continue;
                if (el.type === 'checkbox') el.checked = Boolean(s[key]);
                else el.value = s[key];
            }
        } catch {}
    }

    async function saveSettings() {
        const payload = {};
        for (const el of settingInputs) {
            const key = el.dataset.setting;
            if (el.type === 'checkbox') payload[key] = el.checked;
            else if (el.dataset.type === 'int') payload[key] = parseInt(el.value, 10);
            else payload[key] = el.value.trim();
        }
        const bad = msg => { showToast(msg, 'warning'); };
        if (!Number.isInteger(payload.connections_per_server) || payload.connections_per_server < 1 || payload.connections_per_server > 64)
            return bad('Connections must be between 1 and 64.');
        if (!/^\d+[MKG]$/i.test(payload.min_split_size))
            return bad('Segment size must look like 4M, 8M, 16M…');
        if (!['none', 'prealloc', 'falloc', 'trunc'].includes(payload.file_allocation))
            return bad('Choose a valid file allocation method.');
        if (!/^(0|\d+[KMG])$/i.test(payload.max_download_limit))
            return bad('Speed limit must be 0 or a value like 10M.');
        for (const [key, lo, hi] of [['max_tries', 0, 64], ['retry_wait', 0, 600],
                                     ['connect_timeout', 1, 600], ['timeout', 1, 600]]) {
            const n = payload[key];
            if (!Number.isInteger(n) || n < lo || n > hi)
                return bad(key.replace(/_/g, ' ') + ' must be between ' + lo + ' and ' + hi + '.');
        }
        try {
            await safeFetch('/settings', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(payload)
            });
            showToast('Settings saved — applies to new downloads.', 'success');
            closeSettings();
        } catch {}
    }

    settingsModal.addEventListener('click', e => {
        if (e.target === settingsModal) closeSettings();
    });
    settingsModal.addEventListener('keydown', e => {
        if (e.key !== 'Tab') return;
        const items = [...settingsModal.querySelectorAll(focusableSelector)].filter(el =>
            !el.hidden && el.offsetParent !== null && !el.closest('details:not([open])'));
        if (!items.length) return;
        const first = items[0], last = items[items.length - 1];
        if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
        else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
    });
    document.addEventListener('keydown', e => {
        if (e.key === 'Escape' && settingsModal.classList.contains('show')) closeSettings();
    });

    updateGutter();
    poll();
</script>
</body>
</html>

"""


# ══════════════════════════════════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/health")
def health():
    return jsonify({
        "aria2_rpc": _aria2_healthy,
        "tasks": len(scheduler.get_jobs()),
        "running": any(s.get("status") == "running" for s in job_status.values()),
    })


@app.route("/settings", methods=["GET", "POST"])
def settings():
    if request.method == "GET":
        return jsonify({"settings": _SETTINGS})
    data = request.json or {}
    _apply_settings(data)
    _save_tasks()
    return jsonify({"status": "ok", "settings": _SETTINGS})


@app.route("/autocomplete")
def autocomplete():
    term = os.path.expanduser(request.args.get("term", ""))
    exists = os.path.isdir(term)
    dirname = os.path.dirname(term) or (os.sep if term.startswith(os.sep) else ".")
    prefix = os.path.basename(term)
    paths = []
    if os.path.isdir(dirname):
        try:
            # scandir avoids a stat() per entry; filter first so we only sort
            # the handful of matches instead of the whole (media) directory.
            with os.scandir(dirname) as entries:
                for entry in entries:
                    if entry.name.startswith(prefix) and entry.is_dir():
                        paths.append(entry.path + os.sep)
        except PermissionError:
            pass
        except OSError as e:
            log.warning("autocomplete error: %s", e)
    paths.sort()
    return jsonify({"paths": paths[:12], "exists": exists})


@app.route("/create_path", methods=["POST"])
def create_path():
    raw = request.json.get("path", "")
    path = os.path.expanduser(raw) if raw else ""
    if not path:
        return jsonify({"status": "error", "message": "No path provided"}), 400
    try:
        os.makedirs(path, exist_ok=True)
        return jsonify({"status": "ok"})
    except PermissionError:
        return jsonify({"status": "error", "message": "Permission denied"}), 403
    except OSError as e:
        return jsonify({"status": "error", "message": str(e)}), 400




@app.route("/schedule", methods=["POST"])
def schedule():
    data = request.json
    if not data:
        return jsonify({"status": "error", "message": "Missing JSON body"}), 400

    folder = os.path.abspath(os.path.expanduser(data.get("dest_folder", "")))
    if not folder:
        return jsonify({"status": "error", "message": "Missing dest_folder"}), 400

    urls = [u.strip() for u in data.get("urls", "").splitlines() if u.strip()]
    if not urls:
        return jsonify({"status": "error", "message": "No URLs provided"}), 400

    valid = [u for u in urls if _is_valid_url(u)]
    if not valid:
        return jsonify({"status": "error", "message": "No valid URLs provided"}), 400

    start_now = data.get("start_now", False)
    created_at = time.time()
    job_id = f"job_{int(created_at)}_{uuid.uuid4().hex[:8]}"

    if start_now:
        hour, minute = 0, 0
        cron_data = {"hour": hour, "minute": minute, "is_now": True}
    else:
        try:
            h, m = data["start_time"].split(":")
            hour, minute = int(h), int(m)
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError
        except (KeyError, ValueError, TypeError):
            return jsonify({"status": "error", "message": "Invalid or missing start_time"}), 400
        cron_data = {"hour": hour, "minute": minute, "is_now": False}

    with _cron_lock:
        scheduler.add_job(
            id=job_id,
            func=_queue_task,
            trigger="cron",
            hour=hour,
            minute=minute,
            args=[urls, folder, created_at],
        )
    with _cron_lock:
        job_cron_params[job_id] = cron_data
    with _status_lock:
        job_status[job_id] = {
            "is_now": start_now,
            "dl_paused": False,
            "status": "queued" if start_now else "waiting",
            "current_url": "", "current_file": "",
            "speed": "0 B/s", "progress": 0, "size": "",
            "done": [], "failed": [],
        }

    _save_tasks()
    return jsonify({"status": "ok", "id": job_id})


@app.route("/edit/<jid>", methods=["POST"])
def edit_job(jid):
    data = request.json
    folder = os.path.abspath(os.path.expanduser(data["dest_folder"]))
    urls = [u.strip() for u in data["urls"].splitlines() if u.strip()]
    time_s = data.get("start_time", "")

    old_job = scheduler.get_job(jid)
    if not old_job:
        return jsonify({"status": "error", "message": "Job not found"}), 404

    original_created_at = old_job.args[2]
    with _status_lock:
        old_status = job_status.pop(jid, {})

    with _cron_lock:
        scheduler.remove_job(jid)

    if time_s == "":
        h, m = 0, 0
        is_now = True
    else:
        h, m = time_s.split(":")
        h, m = int(h), int(m)
        is_now = False

    with _cron_lock:
        scheduler.add_job(
            id=jid,
            func=_queue_task,
            trigger="cron",
            hour=h,
            minute=m,
            args=[urls, folder, original_created_at],
        )
    with _cron_lock:
        job_cron_params[jid] = {"hour": h, "minute": m, "is_now": is_now}
    with _status_lock:
        new_status = (
            old_status.get("status", "waiting")
            if old_status.get("status") == "failed"
            else ("queued" if is_now else "waiting")
        )
        job_status[jid] = {
            "is_now": is_now,
            "dl_paused": False,
            "status": new_status,
            "current_url": "", "current_file": "",
            "speed": "0 B/s", "progress": 0, "size": "",
            "done": old_status.get("done", []),
            "failed": old_status.get("failed", [])
            if old_status.get("status") == "failed"
            else [],
        }

    _save_tasks()
    return jsonify({"status": "ok"})


@app.route("/jobs")
def list_jobs():
    jobs = []
    for j in scheduler.get_jobs():
        jid = j.id
        with _cron_lock:
            cron = job_cron_params.get(jid, {"hour": 0, "minute": 0, "is_now": False})
        with _status_lock:
            s = job_status.get(jid, {}).copy()

        time_str = "NOW" if cron.get("is_now") else f"{int(cron.get('hour', 0)):02d}:{int(cron.get('minute', 0)):02d}"

        jobs.append({
            "id": jid,
            "time": time_str,
            "folder": j.args[1],
            "urls": j.args[0],
            "url_total": len(j.args[0]),
            "done_count": len(s.get("done", [])),
            "current_url": s.get("current_url", ""),
            "current_file": s.get("current_file", ""),
            "speed": s.get("speed", "0 B/s"),
            "progress": s.get("progress", 0),
            "size": s.get("size", ""),
            "failed_urls": s.get("failed", []),
            "status": s.get("status", "waiting"),
            "created_at": j.args[2],
            "sched_paused": j.next_run_time is None,
        })
    jobs.sort(key=lambda x: x["created_at"])
    return jsonify(jobs)


@app.route("/control/<action>/<jid>", methods=["POST"])
def control_job(action, jid):
    known = {"delete", "pause", "dl_pause", "dl_resume", "start_now", "retry", "retry_failed", "retry_url"}
    if action not in known:
        return jsonify({"status": "error", "message": f"Unknown action: {action}"}), 400

    if action == "delete":
        with _active_gids_lock:
            gid = active_gids.pop(jid, None)
        if gid:
            _rpc("aria2.remove", [gid])
        with _cron_lock:
            try:
                scheduler.remove_job(jid)
            except Exception as e:
                log.warning("Failed to remove job %s from scheduler: %s", jid, e)
        with _status_lock:
            job_status.pop(jid, None)
        with _cron_lock:
            job_cron_params.pop(jid, None)

    elif action == "pause":
        job = scheduler.get_job(jid)
        if job:
            if job.next_run_time:
                scheduler.pause_job(jid)
            else:
                scheduler.resume_job(jid)

    elif action == "dl_pause":
        with _active_gids_lock:
            gid = active_gids.get(jid)
        if gid:
            _rpc("aria2.forcePause", [gid])
            with _status_lock:
                if jid in job_status:
                    job_status[jid]["status"] = "paused"
                    job_status[jid]["dl_paused"] = True

    elif action == "dl_resume":
        with _active_gids_lock:
            gid = active_gids.get(jid)
        if gid:
            _rpc("aria2.unpause", [gid])
            with _status_lock:
                if jid in job_status:
                    job_status[jid]["status"] = "running"
                    job_status[jid]["dl_paused"] = False

    elif action == "start_now":
        with _status_lock:
            if jid in job_status:
                job_status[jid]["is_now"] = True
                job_status[jid]["status"] = "queued"
                job_status[jid]["dl_paused"] = False
        with _cron_lock:
            if jid in job_cron_params:
                job_cron_params[jid]["is_now"] = True

    elif action == "retry":
        with _status_lock:
            if jid in job_status and job_status[jid].get("failed"):
                job_status[jid]["status"] = "queued"
                job_status[jid]["dl_paused"] = False

    elif action == "retry_failed":
        with _status_lock:
            if jid in job_status and job_status[jid].get("failed"):
                failed = list(job_status[jid].get("failed", []))
                job_status[jid]["status"] = "queued"
                job_status[jid]["dl_paused"] = False
                job_status[jid]["failed"] = []
                job_status[jid]["_retry_only"] = failed

    elif action == "retry_url":
        url = (request.json or {}).get("url", "")
        with _status_lock:
            st = job_status.get(jid)
            if st and url and url in st.get("failed", []):
                st["_retry_only"] = [url]
                st["status"] = "queued"
                st["dl_paused"] = False

    _save_tasks()
    return jsonify({"status": "ok"})


# ══════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════

def _parse_args():
    parser = argparse.ArgumentParser(description="Aria2 Scheduler Web UI")
    parser.add_argument("--host", default=os.environ.get("HOST", "127.0.0.1"),
                        help="Listen address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "5000")),
                        help="Listen port (default: 5000)")
    parser.add_argument("--debug", action="store_true", default=False,
                        help="Enable debug mode")
    return parser.parse_args()


def main():
    args = _parse_args()

    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
    global log
    log = logging.getLogger("aria2-webui")

    _start_aria2_daemon()
    scheduler.init_app(app)
    scheduler.start()
    _load_tasks()

    # Start supervisor thread
    threading.Thread(target=_download_supervisor, daemon=True).start()

    # Use waitress if available, else fall back to Flask dev server
    try:
        import waitress
        log.info("Starting with waitress on %s:%d", args.host, args.port)
        waitress.serve(app, host=args.host, port=args.port, threads=8)
    except ImportError:
        log.warning("waitress not installed — using Flask dev server (not for production)")
        app.run(host=args.host, port=args.port, debug=args.debug, use_reloader=False)


if __name__ == "__main__":
    main()