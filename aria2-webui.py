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

_ARIA2_RPC = os.environ.get("ARIA2_RPC", "http://localhost:6800/jsonrpc")
_ARIA2_SECRET = os.environ.get("ARIA2_SECRET", "")
_ARIA2_PORT = _env_int("ARIA2_PORT", 6800)
_DB_FILE = os.environ.get("DB_FILE", "aria_tasks.json")
_DOWNLOAD_STALL_SECONDS = _env_int("DOWNLOAD_STALL_SECONDS", 300)

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
        r = requests.post(_ARIA2_RPC, json=payload, timeout=5)
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
            f.write("split=1\n")
            f.write("max-connection-per-server=1\n")
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
        "split": "1",
        "max-connection-per-server": "1",
        "check-certificate": "false",
    }
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
                json.dump(tasks, f, indent=2)
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
            tasks = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.error("Corrupted task file: %s", e)
        return

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
            job_status[job_id].update({
                "status": "running",
                "dl_paused": False,
                "current_url": "",
                "current_file": "",
                "speed": "0 B/s",
                "progress": 0,
                "size": "",
                "done": job_status[job_id].get("done", []),
                "failed": [],
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
<html>
<head>
    <title>Aria2 Scheduler</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'><rect width='16' height='16' rx='3' fill='%230d1117'/><text x='8' y='13' text-anchor='middle' font-size='12' fill='%2358a6ff'>⬇</text></svg>">
    <style>
        :root {
            --bg:#0d1117; --card:#161b22; --text:#c9d1d9;
            --blue:#58a6ff; --green:#3fb950; --red:#f85149;
            --orange:#d29922; --purple:#bc8cff;
        }
        body {
            font-family:'Consolas',monospace;
            background:var(--bg); color:var(--text);
            max-width:800px; margin:20px auto; padding:20px;
        }
        .card {
            background:var(--card); padding:20px;
            border-radius:6px; border:1px solid #30363d; margin-bottom:20px;
        }
        .card.editing {
            border-color:var(--orange);
            box-shadow:0 0 0 2px rgba(210,153,34,0.25);
        }
        h2 {
            color:var(--blue); margin:0 0 15px 0;
            font-size:1rem; border-bottom:1px solid #30363d; padding-bottom:10px;
        }
        h2.editing-title { color:var(--orange); }

        input, textarea {
            width:100%; background:#0d1117; border:1px solid #30363d;
            color:#fff; padding:10px; border-radius:4px;
            box-sizing:border-box; font-family:inherit;
        }
        input:focus, textarea:focus { outline:none; border-color:var(--blue); }

        /* Path input group — input + buttons as one visual unit */
        .path-input-group {
            display:flex; align-items:stretch; position:relative;
            border:1px solid #30363d; border-radius:4px;
            background:#0d1117;
            transition:border-color 0.2s;
        }
        .path-input-group:focus-within { border-color:var(--blue); }
        .path-input-group input {
            flex:1; min-width:0;
            border:none; background:transparent;
            color:#fff; padding:10px; font-family:inherit;
            font-size:inherit; outline:none;
        }
        .path-input-group .btn-history {
            flex-shrink:0; border:none; border-left:1px solid #30363d;
            border-radius:0; background:#0d1117; color:#8b949e;
            padding:0 11px; cursor:pointer; font-size:0.8rem;
            user-select:none; line-height:1;
            transition:color 0.15s, background 0.15s;
        }
        .path-input-group .btn-history:hover { color:var(--blue); background:#161b22; }
        .path-input-group .btn-history:only-child {
            border-radius:0 4px 4px 0;
        }
        .path-input-group .btn-reuse {
            flex-shrink:0; border:none; border-left:1px solid #30363d;
            border-radius:0 4px 4px 0; background:#0d1117; color:var(--blue);
            padding:0 14px; cursor:pointer; font-size:0.75rem;
            white-space:nowrap; user-select:none;
            transition:background 0.15s;
        }
        .path-input-group .btn-reuse:hover  { background:#161b22; }
        .path-input-group .btn-reuse:active { background:#21262d; }

        /* Path history dropdown */
        .path-history-drop {
            display:none; position:absolute; top:calc(100% + 4px); right:0;
            background:var(--card); border:1px solid #30363d;
            border-radius:4px; z-index:10; min-width:260px;
            box-shadow:0 4px 12px rgba(0,0,0,0.5);
            max-height:200px; overflow-y:auto;
        }
        .path-history-drop.show { display:block; }
        .path-history-item {
            padding:8px 12px; cursor:pointer; font-size:0.75rem;
            color:#8b949e; border-bottom:1px solid #21262d;
            word-break:break-all;
        }
        .path-history-item:last-child { border-bottom:none; }
        .path-history-item:hover { color:var(--blue); background:#1a2030; }
        .path-history-clear {
            padding:6px 12px; cursor:pointer; font-size:0.65rem;
            color:var(--red); text-align:center;
        }
        .path-history-clear:hover { background:#1a2030; }

        #suggestion-box { display:flex; flex-wrap:wrap; gap:6px; margin-top:8px; min-height:10px; }
        .sugg-item {
            background:#21262d; padding:4px 10px; border-radius:20px;
            cursor:pointer; font-size:0.75rem; border:1px solid #30363d;
            user-select:none;
        }
        .sugg-item:hover { border-color:var(--blue); }

        #path-status { display:none; align-items:center; gap:10px; margin-top:8px; }
        .btn-create {
            background:var(--orange); color:black; border:none;
            padding:5px 14px; border-radius:4px; cursor:pointer;
            font-size:0.75rem; font-weight:bold;
        }

        /* URL count indicator */
        .url-count {
            font-size:0.7rem; color:#8b949e; margin-top:6px;
            display:flex; gap:12px; align-items:center;
        }
        .url-count .invalid { color:var(--red); }
        .url-count .total   { color:var(--green); }

        .progress-wrap { background:#21262d; border-radius:4px; height:6px; margin-top:8px; overflow:hidden; }
        .progress-bar  { height:100%; background:var(--green); border-radius:4px; transition:width 0.4s ease; }
        .progress-bar.paused { background:var(--orange); }

        .job-card { background:#0d1117; border-left:4px solid var(--blue); padding:12px 15px; margin-bottom:12px; border-radius:0 4px 4px 0; transition:border-color 0.2s, opacity 0.2s; }
        .job-card.being-edited { border-left-color:var(--orange); opacity:0.6; }
        .job-header { display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:6px; }
        .job-footer { margin-top:8px; font-size:0.75rem; color:#8b949e; border-top:1px solid #21262d; padding-top:8px; }

        .status-badge   { padding:2px 8px; border-radius:10px; font-size:0.65rem; font-weight:bold; }
        .status-waiting   { background:#21262d; color:#8b949e; }
        .status-queued    { background:#1f3d5a; color:var(--blue); }
        .status-running   { background:#1f3d1a; color:var(--green); }
        .status-paused    { background:#3d2e00; color:var(--orange); }
        .status-dl_paused { background:#3d2e00; color:var(--orange); }
        .status-failed    { background:#3d1a1a; color:var(--red); }
        .status-done      { background:#1a3d2a; color:var(--green); }
        .status-editing   { background:#3d2e00; color:var(--orange); }

        .btn-row  { display:flex; gap:5px; flex-shrink:0; flex-wrap:wrap; }
        .btn      { border:none; padding:5px 10px; border-radius:4px; cursor:pointer; font-weight:bold; font-size:0.7rem; }
        .btn-pause-sched { background:var(--orange); color:black; }
        .btn-dl-pause    { background:var(--orange); color:black; }
        .btn-dl-resume   { background:var(--green);  color:black; }
        .btn-del         { background:var(--red);    color:white; }
        .btn-retry       { background:var(--blue);   color:black; }
        .btn-retry-fail  { background:#1a3d5a; color:var(--blue); border:1px solid var(--blue); }
        .btn-start       { background:var(--green);  color:black; }
        .btn-edit        { background:var(--purple);  color:black; }

        .form-btns    { display:flex; gap:10px; margin-top:12px; }
        .btn-schedule { background:#21262d; color:var(--blue); border:1px solid var(--blue); flex:1; padding:12px; border-radius:4px; cursor:pointer; font-weight:bold; font-size:0.85rem; }
        .btn-now      { background:var(--green);  color:black; border:none; flex:1; padding:12px; border-radius:4px; cursor:pointer; font-weight:bold; font-size:0.85rem; }
        .btn-save     { background:var(--orange); color:black; border:none; flex:1; padding:12px; border-radius:4px; cursor:pointer; font-weight:bold; font-size:0.85rem; }
        .btn-cancel   { background:#21262d; color:var(--red); border:1px solid var(--red); flex:1; padding:12px; border-radius:4px; cursor:pointer; font-weight:bold; font-size:0.85rem; }
        .btn-schedule:hover { background:var(--blue); color:black; }
        .btn-now:hover, .btn-save:hover { filter:brightness(1.15); }

        .failed-url  { color:var(--red);   font-size:0.7rem; padding:2px 0; word-break:break-all; }
        .current-url { color:var(--green); font-size:0.7rem; word-break:break-all; margin-bottom:5px; }
        .paused-url  { color:var(--orange); font-size:0.7rem; word-break:break-all; margin-bottom:5px; }
        .speed-line  { display:flex; justify-content:space-between; color:#8b949e; font-size:0.7rem; margin-top:5px; }
        .edit-hint   { font-size:0.75rem; color:var(--orange); margin-top:6px; }

        /* ── Responsive ─────────────────────────────── */
        @media (max-width:600px) {
            body { max-width:100%; margin:0; padding:12px; font-size:0.9rem; }
            .card { padding:14px; border-radius:4px; }
            .path-input-group { border-radius:3px; }
            .path-input-group .btn-history,
            .path-input-group .btn-reuse { padding:0 10px; font-size:0.7rem; }
            .job-header { flex-direction:column; align-items:flex-start; }
            .btn-row { width:100%; }
            .btn { padding:6px 12px; font-size:0.72rem; flex:1; text-align:center; }
            .form-btns { flex-direction:column; gap:8px; }
            .btn-schedule, .btn-now, .btn-save, .btn-cancel { padding:14px; }
            .sugg-item { font-size:0.7rem; padding:3px 8px; }
            h2 { font-size:0.9rem; }
            .path-history-drop { min-width:200px; left:0; right:auto; }
        }

        /* Toast notifications */
        #toast-container { position:fixed; top:16px; right:16px; z-index:9999; display:flex; flex-direction:column; gap:8px; max-width:380px; }
        .toast {
            background:var(--card); border:1px solid #30363d; border-radius:6px;
            padding:12px 16px; font-size:0.8rem; color:var(--text);
            box-shadow:0 4px 16px rgba(0,0,0,0.5); animation:toastIn 0.25s ease;
            display:flex; gap:10px; align-items:flex-start;
        }
        .toast.error   { border-color:var(--red); }
        .toast.warning { border-color:var(--orange); }
        .toast.success { border-color:var(--green); }
        .toast .toast-icon { flex-shrink:0; font-size:1rem; line-height:1; }
        .toast .toast-msg  { flex:1; word-break:break-word; }
        @keyframes toastIn { from{opacity:0;transform:translateY(-10px)} to{opacity:1;transform:translateY(0)} }
        @media (max-width:600px) { #toast-container { left:12px; right:12px; max-width:none; } }
    </style>
</head>
<body>

<div id="toast-container"></div>

<div class="card" id="form-card">
    <h2 id="form-title">NEW TASK</h2>

    <div class="path-input-group">
        <input type="text" id="dest_folder" autocomplete="off" placeholder="Destination Path  (Tab to complete)">
        <button type="button" class="btn-history" id="btn-history"
                title="Recent paths" onclick="togglePathHistory()">&#9660;</button>
        <button type="button" class="btn-reuse" id="btn-reuse"
                title="Reload last used path" onclick="reuseLastPath()" style="display:none;">
            &#8635; Reuse
        </button>
        <div class="path-history-drop" id="path-history-drop"></div>
    </div>
    <div id="path-status">
        <span style="font-size:0.75rem;color:#8b949e;">Path does not exist.</span>
        <button type="button" class="btn-create" onclick="createPath()">CREATE FOLDER</button>
    </div>
    <div id="suggestion-box"></div>

    <textarea id="urls" rows="5" style="margin-top:12px;"
              placeholder="Paste URLs here, one per line..."></textarea>
    <div class="url-count" id="url-count"></div>

    <div style="margin-top:12px;">
        <input type="time" id="start_time">
    </div>

    <div class="form-btns" id="btns-normal">
        <button class="btn-schedule" onclick="submitTask('schedule')">SCHEDULE</button>
        <button class="btn-now"      onclick="submitTask('now')">START NOW</button>
    </div>
    <div class="form-btns" id="btns-edit" style="display:none;">
        <button class="btn-save"   onclick="saveEdit()">SAVE CHANGES</button>
        <button class="btn-cancel" onclick="cancelEdit()">CANCEL</button>
    </div>
</div>

<div id="job-list"></div>

<script>
    let suggestions  = [];
    let editingJobId = null;

    const destInput     = document.getElementById('dest_folder');
    const suggBox       = document.getElementById('suggestion-box');
    const pathStatus    = document.getElementById('path-status');
    const formCard      = document.getElementById('form-card');
    const formTitle     = document.getElementById('form-title');
    const btnsNormal    = document.getElementById('btns-normal');
    const btnsEdit      = document.getElementById('btns-edit');
    const urlsTextarea  = document.getElementById('urls');
    const urlCountEl    = document.getElementById('url-count');
    const pathHistDrop  = document.getElementById('path-history-drop');

    const BASE_TITLE = 'Aria2 Scheduler';

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
        if (lastUsedPath) {
            btn.style.display = '';
            btn.title = 'Reuse: ' + lastUsedPath;
            histBtn.style.borderRadius = '0';
        } else {
            btn.style.display = 'none';
            histBtn.style.borderRadius = '0 4px 4px 0';
        }
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

    async function checkPath(val) {
        if (!val) { suggBox.innerHTML = ""; pathStatus.style.display = "none"; return; }
        const res  = await fetch('/autocomplete?term=' + encodeURIComponent(val));
        const data = await res.json();
        suggestions = data.paths;

        suggBox.innerHTML = suggestions.map(s => {
            const label = s.split('/').filter(x => x).pop() || s;
            return '<span class="sugg-item" data-path="' + escapeHtml(s) + '">' + escapeHtml(label) + '</span>';
        }).join('');

        suggBox.querySelectorAll('.sugg-item').forEach(el => {
            el.addEventListener('mousedown', e => {
                e.preventDefault();
                destInput.value = el.dataset.path;
                checkPath(el.dataset.path);
            });
        });

        pathStatus.style.display = (!data.exists && suggestions.length === 0) ? "flex" : "none";
    }

    function escapeHtml(str) {
        const div = document.createElement('div');
        div.appendChild(document.createTextNode(str));
        return div.innerHTML;
    }

    function showToast(msg, type) {
        type = type || 'error';
        const icons = {error:'\u2717', warning:'\u26A0', success:'\u2713'};
        const container = document.getElementById('toast-container');
        const toast = document.createElement('div');
        toast.className = 'toast ' + type;
        toast.innerHTML = '<span class="toast-icon">' + icons[type] + '</span>' +
                          '<span class="toast-msg">' + escapeHtml(msg) + '</span>';
        container.appendChild(toast);
        setTimeout(() => { toast.style.opacity = '0'; toast.style.transition = 'opacity 0.3s'; }, 3500);
        setTimeout(() => toast.remove(), 3800);
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
            if (e.name === 'TypeError' && e.message.includes('fetch')) {
                showToast('Cannot reach server. Is the app running?', 'error');
            } else {
                showToast(e.message, 'error');
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
        const valid = urls.filter(isValidUrl).length;
        const invalid = total - valid;
        if (!total) { urlCountEl.innerHTML = ''; return; }
        let html = '<span class="total">' + total + ' URL' + (total !== 1 ? 's' : '') + '</span>';
        if (invalid) html += '<span class="invalid">' + invalid + ' invalid</span>';
        urlCountEl.innerHTML = html;
    }

    urlsTextarea.addEventListener('input', updateUrlCount);
    urlsTextarea.addEventListener('paste', () => setTimeout(updateUrlCount, 50));

    // ── Edit mode ────────────────────────────────────────

    function enterEditMode(job) {
        editingJobId = job.id;
        destInput.value = job.folder;
        urlsTextarea.value = job.urls.join('\n');

        document.getElementById('start_time').value = (job.time === "NOW" ? "" : job.time);

        formCard.classList.add('editing');
        formTitle.textContent = 'EDITING TASK';
        formTitle.classList.add('editing-title');
        btnsNormal.style.display = 'none';
        btnsEdit.style.display   = 'flex';
        window.scrollTo({top: 0, behavior: 'smooth'});
        checkPath(job.folder);
        updateUrlCount();
        refreshJobs();
    }

    function cancelEdit() {
        editingJobId = null;
        clearForm();
        formCard.classList.remove('editing');
        formTitle.textContent = 'NEW TASK';
        formTitle.classList.remove('editing-title');
        btnsNormal.style.display = 'flex';
        btnsEdit.style.display   = 'none';
        refreshJobs();
    }

    function clearForm() {
        destInput.value = '';
        urlsTextarea.value = '';
        document.getElementById('start_time').value = '';
        suggBox.innerHTML = '';
        pathStatus.style.display = 'none';
        urlCountEl.innerHTML = '';
    }

    async function saveEdit() {
        const folder = destInput.value.trim();
        const urls   = urlsTextarea.value.trim();

        if (!folder) { showToast("Please enter a destination path.", "warning"); return; }
        if (!urls)   { showToast("Please enter at least one URL.", "warning");   return; }

        await safeFetch('/edit/' + editingJobId, {
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

        if (!folder) { showToast("Please enter a destination path.", "warning"); return; }
        if (!urls)   { showToast("Please enter at least one URL.", "warning");   return; }
        if (mode === 'schedule' && !t) { showToast("Please select a time.", "warning"); return; }

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

    // ── Job list ─────────────────────────────────────────

    function badge(status) {
        return '<span class="status-badge status-' + status + '">' + status.toUpperCase() + '</span>';
    }

    async function refreshJobs() {
        try {
            const res  = await safeFetch('/jobs');
            const jobs = await res.json();
            refreshJobsInner(jobs);
        } catch {}
    }

    async function editJob(id) {
        try {
            const res  = await safeFetch('/jobs');
            const jobs = await res.json();
            const job  = jobs.find(j => j.id === id);
            if (job) enterEditMode(job);
        } catch {}
    }

    async function control(id, action) {
        try {
            await safeFetch('/control/' + action + '/' + id, {method: 'POST'});
        } catch {}
        refreshJobs();
    }

    function updateTabTitle(jobs) {
        const running  = jobs.filter(j => j.status === 'running').length;
        const paused   = jobs.filter(j => j.status === 'paused' || j.status === 'dl_paused').length;
        const failed   = jobs.filter(j => j.status === 'failed').length;
        let prefix = '';
        if (running)      prefix = '\u2B07 ';  // ⬇
        else if (paused)  prefix = '\u23F8 ';  // ⏸
        else if (failed)  prefix = '\u2717 ';  // ✗
        document.title = prefix + BASE_TITLE;
    }

    async function adaptiveRefresh() {
        try {
            const res  = await safeFetch('/jobs');
            const jobs = await res.json();
            refreshJobsInner(jobs);
            updateTabTitle(jobs);
            const busy = jobs.some(j => j.status === 'running' || j.status === 'paused' || j.status === 'dl_paused');
            setTimeout(adaptiveRefresh, busy ? 800 : 4000);
        } catch {
            setTimeout(adaptiveRefresh, 6000);
        }
    }

    function refreshJobsInner(jobs) {
        document.getElementById('job-list').innerHTML = jobs.length === 0
            ? '<div style="color:#555;text-align:center;padding:20px;">No tasks scheduled.</div>'
            : jobs.map(j => {
                const isRunning  = j.status === 'running';
                const isPaused   = j.status === 'paused' || j.status === 'dl_paused';
                const isFailed   = j.status === 'failed';
                const isWaiting  = j.status === 'waiting' || j.status === 'queued';
                const isActive   = isRunning || isPaused;
                const isEditing  = j.id === editingJobId;
                const hasBoth    = isFailed && j.failed_urls.length && j.done_count > 0;

                const progressSection = isActive ? `
                    <div class="${isPaused ? 'paused-url' : 'current-url'}">
                        ${isPaused ? '&#9646;&#9646;' : '&#8595;'} ${escapeHtml(j.current_file || j.current_url)}
                    </div>
                    <div class="progress-wrap">
                        <div class="progress-bar ${isPaused ? 'paused' : ''}"
                             style="width:${parseFloat(j.progress) || 0}%"></div>
                    </div>
                    <div class="speed-line">
                        <span>${parseFloat(j.progress) || 0}%  &mdash;  ${escapeHtml(j.size)}</span>
                        <span>${isPaused ? 'PAUSED' : escapeHtml(j.speed)}</span>
                    </div>
                    <div style="margin-top:6px;color:#8b949e;font-size:0.7rem">
                        ${j.done_count} / ${j.url_total} files done
                    </div>` : '';

                const failedSection = isFailed && j.failed_urls.length ? `
                    <div style="color:var(--red);margin-bottom:4px;">Failed URLs:</div>
                    ${j.failed_urls.map(u => '<div class="failed-url">&times; ' + escapeHtml(u) + '</div>').join('')}` : '';

                const editHint = isEditing
                    ? '<div class="edit-hint">&#9998; Currently being edited&hellip;</div>' : '';

                let btns = '';
                if (!isEditing) {
                    if (isWaiting) {
                        btns += '<button class="btn btn-start" onclick="control(\'' + j.id + '\',\'start_now\')">START NOW</button>';
                        btns += '<button class="btn btn-pause-sched" onclick="control(\'' + j.id + '\',\'pause\')">' + (j.sched_paused ? 'RESUME' : 'PAUSE') + '</button>';
                    }
                    if (isRunning) {
                        btns += '<button class="btn btn-dl-pause" onclick="control(\'' + j.id + '\',\'dl_pause\')">PAUSE DL</button>';
                    }
                    if (isPaused) {
                        btns += '<button class="btn btn-dl-resume" onclick="control(\'' + j.id + '\',\'dl_resume\')">RESUME DL</button>';
                    }
                    if (!isActive) {
                        btns += '<button class="btn btn-edit" onclick="editJob(\'' + j.id + '\')">EDIT</button>';
                    }
                    btns += '<button class="btn btn-del" onclick="control(\'' + j.id + '\',\'delete\')">DEL</button>';
                    if (isFailed) {
                        btns += '<button class="btn btn-retry" onclick="control(\'' + j.id + '\',\'retry\')">RETRY</button>';
                        if (hasBoth) {
                            btns += '<button class="btn btn-retry-fail" onclick="control(\'' + j.id + '\',\'retry_failed\')">RETRY FAILED</button>';
                        }
                    }
                }

                return `
                <div class="job-card ${isEditing ? 'being-edited' : ''}">
                    <div class="job-header">
                        <div>
                            <b style="color:var(--blue)">[${escapeHtml(j.time)}]</b>
                            ${badge(j.status)}
                            <span style="color:#8b949e;font-size:0.8rem;margin-left:6px">
                                ${j.url_total} URL${j.url_total !== 1 ? 's' : ''} &mdash; ${escapeHtml(j.folder)}
                            </span>
                        </div>
                        <div class="btn-row">${btns}</div>
                    </div>
                    ${editHint}
                    ${isActive || isFailed
                        ? '<div class="job-footer">' + progressSection + failedSection + '</div>'
                        : ''}
                </div>`;
            }).join('');
    }

    adaptiveRefresh();
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


@app.route("/autocomplete")
def autocomplete():
    term = os.path.expanduser(request.args.get("term", ""))
    exists = os.path.isdir(term)
    dirname = os.path.dirname(term) or (os.sep if term.startswith(os.sep) else ".")
    prefix = os.path.basename(term)
    paths = []
    try:
        if os.path.exists(dirname):
            for f in sorted(os.listdir(dirname)):
                if f.startswith(prefix):
                    full = os.path.join(dirname, f)
                    if os.path.isdir(full):
                        paths.append(full + os.sep)
    except PermissionError:
        pass
    except OSError as e:
        log.warning("autocomplete error: %s", e)
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
    known = {"delete", "pause", "dl_pause", "dl_resume", "start_now", "retry", "retry_failed"}
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