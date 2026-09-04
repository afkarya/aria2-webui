#!/usr/bin/env bash
#
# aria2-webui setup script
#
# Installs the web UI, its Python dependencies and a system service.
# If no aria2 RPC server is available, the app manages its own built-in
# aria2c daemon (see _start_aria2_daemon in aria2-webui.py).
#
set -euo pipefail

APP_NAME="aria2-webui"
APP_DIR="/opt/${APP_NAME}"
SERVICE_NAME="${APP_NAME}"
DEFAULT_RPC_URL="http://localhost:6800/jsonrpc"
DEFAULT_HOST="127.0.0.1"
DEFAULT_PORT=5000

NONINTERACTIVE=0
UNINSTALL=0
HOST_ARG=""
PORT_ARG=""
RPC_URL_ARG=""
SECRET_ARG=""

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
ORIG_ARGS=("$@")

# ── Output helpers ─────────────────────────────────────────────────

info() { printf '\033[1;34m[%s]\033[0m %s\n' "$APP_NAME" "$*"; }
ok()   { printf '\033[1;32m[ ok ]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

usage() {
  cat <<EOF
Usage: $0 [options]

Installs ${APP_NAME} to ${APP_DIR} and runs it as a background service.

Options:
  --non-interactive   Ask no questions (defaults / existing config are used)
  --host HOST         Bind address for the web UI (default: ${DEFAULT_HOST})
  --port PORT         Port for the web UI (default: ${DEFAULT_PORT})
  --rpc-url URL       Existing aria2 JSON-RPC endpoint
  --rpc-secret SECRET Secret token for that existing RPC endpoint
  --uninstall         Stop the service, delete ${APP_DIR} and its config
  -h, --help          Show this help

Supported distros: Debian/Ubuntu (apt), Arch (pacman), Fedora/RHEL (dnf),
OpenWrt (opkg).
EOF
  exit 0
}

# ── Argument parsing ───────────────────────────────────────────────

while [ $# -gt 0 ]; do
  case "$1" in
    --non-interactive) NONINTERACTIVE=1; shift ;;
    --host)           HOST_ARG="${2:?--host requires a value}"; shift 2 ;;
    --port)           PORT_ARG="${2:?--port requires a value}"; shift 2 ;;
    --rpc-url)        RPC_URL_ARG="${2:?--rpc-url requires a value}"; shift 2 ;;
    --rpc-secret)     SECRET_ARG="${2:?--rpc-secret requires a value}"; shift 2 ;;
    --uninstall)      UNINSTALL=1; shift ;;
    -h|--help)        usage ;;
    *) fail "Unknown option: $1 (see --help)" ;;
  esac
done

# ── Privilege check (re-run with sudo when needed) ─────────────────

if [ "$(id -u)" -ne 0 ]; then
  if command -v sudo >/dev/null 2>&1; then
    info "Re-running with sudo…"
    exec sudo -E bash "$SCRIPT_DIR/$(basename "$0")" "${ORIG_ARGS[@]}"
  fi
  fail "Please run as root, e.g. 'sudo bash $0'."
fi

# ── Distro detection ───────────────────────────────────────────────

DISTRO=""

detect_distro() {
  local id like
  # shellcheck disable=SC1091
  . /etc/os-release 2>/dev/null || fail "Cannot detect the OS (/etc/os-release is missing)."
  id="${ID:-}"
  case "$id" in
    debian|ubuntu|raspbian|linuxmint|pop|elementary) DISTRO="apt"; return ;;
    arch|manjaro|endeavouros|garuda)                 DISTRO="pacman"; return ;;
    fedora|rhel|centos|rocky|almalinux|ol)            DISTRO="dnf"; return ;;
    openwrt)                                          DISTRO="opkg"; return ;;
  esac
  for like in ${ID_LIKE:-}; do
    case "$like" in
      debian|ubuntu) DISTRO="apt"; return ;;
      arch)          DISTRO="pacman"; return ;;
      fedora|rhel)   DISTRO="dnf"; return ;;
    esac
  done
  fail "Unsupported distro '${id:-unknown}'. Supported: Debian/Ubuntu, Arch, Fedora, OpenWrt."
}

# ── Small helpers ──────────────────────────────────────────────────

ask_yes() {  # ask_yes "prompt" [y|n] → 0 if yes
  local prompt="$1" default="${2:-n}" ans
  case "$default" in
    y|Y) prompt="$prompt [Y/n]"; default=y ;;
    *)   prompt="$prompt [y/N]"; default=n ;;
  esac
  while true; do
    printf '%s ' "$prompt"
    read -r ans || return 1
    ans="${ans:-$default}"
    case "$ans" in
      y|Y|yes|YES) return 0 ;;
      n|N|no|NO)   return 1 ;;
    esac
    printf '%s\n' "Please answer 'y' or 'n'."
  done
}

ask_str() {     # ask_str "prompt" "default" → value on stdout
  local prompt="$1" default="$2" ans
  if [ -n "$default" ]; then prompt="$prompt [$default]"; fi
  printf '%s: ' "$prompt"
  read -r ans || true
  printf '%s\n' "${ans:-$default}"
}

gen_secret() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 16
  else
    head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n'
  fi
}

http_ok() {     # http_ok URL → 0 if server answers
  if command -v curl >/dev/null 2>&1; then
    curl -m 2 -fsS "$1" >/dev/null 2>&1
  else
    wget -qO- --timeout=3 "$1" >/dev/null 2>&1
  fi
}

detect_rpc() {  # detect_rpc URL → echo none|open|secret
  local url="$1" resp code
  resp="$(curl -m 4 -s -X POST "$url" \
          -H 'Content-Type: application/json' \
          --data '{"jsonrpc":"2.0","id":"probe","method":"aria2.getVersion","params":[]}' \
          2>/dev/null || true)"
  [ -z "$resp" ] && { echo "none"; return; }
  # Not a JSON-RPC response at all (wrong server / proxy / HTML page)
  printf '%s' "$resp" | grep -q '"jsonrpc"' || { echo "none"; return; }
  code="$(printf '%s' "$resp" | sed -n 's/.*"code"[[:space:]]*:[[:space:]]*\([0-9]*\).*/\1/p' | head -n1)"
  # aria2 replies with code 1 when the secret token is required
  if [ "$code" = "1" ]; then echo "secret"; else echo "open"; fi
}

env_get() {     # env_get KEY .env → value
  sed -n "s/^${1}=//p" "$2" 2>/dev/null | tail -n1 \
    | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//"
}

# ── Package installation ───────────────────────────────────────────

install_packages() {
  case "$DISTRO" in
    apt)
      info "Installing aria2, Python and curl (apt)…"
      apt-get update -qq
      apt-get install -y aria2 python3 python3-venv python3-pip curl
      ;;
    pacman)
      info "Installing aria2, Python and curl (pacman)…"
      pacman -S --noconfirm --needed aria2 python python-pip curl
      ;;
    dnf)
      info "Installing aria2, Python and curl (dnf)…"
      dnf install -y aria2 python3 python3-pip curl
      ;;
    opkg)
      info "Installing Python and curl (opkg)…"
      opkg update
      opkg install python3 curl \
        || warn "Failed to install python3/curl via opkg — continuing anyway."
      info "Installing aria2 (opkg)…"
      opkg install aria2 \
        || warn "aria2 not found in opkg feeds — the app can still use an external RPC server."
      info "Installing python3-pip (opkg)…"
      opkg install python3-pip \
        || warn "python3-pip unavailable — will use pip/venv fallbacks if present."
      ;;
  esac
  command -v aria2c >/dev/null 2>&1 \
    || warn "aria2c not found after install — the app can only use an external RPC server."
}

# ── Application install + Python deps ──────────────────────────────

install_deps() {
  local reqs="$APP_DIR/requirements.txt"
  if [ -x "$APP_DIR/venv/bin/python3" ]; then
    ok "Reusing existing virtualenv."
    PYTHON_BIN="$APP_DIR/venv/bin/python3"
    "$PYTHON_BIN" -m pip install -q --upgrade pip || true
    "$PYTHON_BIN" -m pip install -q -r "$reqs" \
      || warn "Dependency upgrade failed — continuing with what is installed."
    return
  fi
  if python3 -m venv --help >/dev/null 2>&1; then
    info "Creating Python virtualenv…"
    if python3 -m venv "$APP_DIR/venv" 2>/dev/null && [ -x "$APP_DIR/venv/bin/python3" ]; then
      PYTHON_BIN="$APP_DIR/venv/bin/python3"
      "$PYTHON_BIN" -m pip install -q --upgrade pip || true
      "$PYTHON_BIN" -m pip install -q -r "$reqs" \
        || "$PYTHON_BIN" -m pip install -r "$reqs" \
        || fail "Failed to install Python dependencies."
      return
    fi
    warn "venv creation failed — falling back to system-wide pip."
    rm -rf "$APP_DIR/venv"
  fi
  warn "python3-venv unavailable — installing dependencies system-wide."
  PYTHON_BIN="$(command -v python3 || true)"
  [ -n "$PYTHON_BIN" ] || fail "python3 is not installed."
  pip3 install -q --break-system-packages -r "$reqs" 2>/dev/null && return
  pip3 install -q -r "$reqs" 2>/dev/null && return
  pip install -q --break-system-packages -r "$reqs" 2>/dev/null && return
  pip install -q -r "$reqs" || fail "Failed to install Python dependencies."
}

install_into_opt() {
  local req
  for req in aria2-webui.py requirements.txt; do
    [ -f "$SCRIPT_DIR/$req" ] \
      || fail "Missing $req in $SCRIPT_DIR — run this script from the project directory."
  done
  mkdir -p "$APP_DIR"
  if command -v systemctl >/dev/null 2>&1 && systemctl -q is-active "$SERVICE_NAME" 2>/dev/null; then
    systemctl stop "$SERVICE_NAME" || true
  elif [ -e "/etc/init.d/${SERVICE_NAME}" ]; then
    "/etc/init.d/${SERVICE_NAME}" stop 2>/dev/null || true
  fi
  cp -f "$SCRIPT_DIR/aria2-webui.py" "$SCRIPT_DIR/requirements.txt" "$APP_DIR/"
  [ -f "$SCRIPT_DIR/.env.example" ] && cp -f "$SCRIPT_DIR/.env.example" "$APP_DIR/" || true
  [ -f "$SCRIPT_DIR/LICENSE" ] && cp -f "$SCRIPT_DIR/LICENSE" "$APP_DIR/" || true
  ok "Copied app files to $APP_DIR."
  install_deps
}

# ── Configuration decision ─────────────────────────────────────────

CONFIG_HOST="" CONFIG_PORT="" CONFIG_RPC="" CONFIG_SECRET="" MODE="builtin"

decide_config() {
  local prior_host prior_port prior_rpc prior_secret detected keep default_ons
  prior_host=""; prior_port=""; prior_rpc=""; prior_secret=""
  if [ -f "$APP_DIR/.env" ]; then
    prior_rpc="$(env_get ARIA2_RPC "$APP_DIR/.env")"
    prior_secret="$(env_get ARIA2_SECRET "$APP_DIR/.env")"
    prior_host="$(env_get HOST "$APP_DIR/.env")"
    prior_port="$(env_get PORT "$APP_DIR/.env")"
  fi

  keep=0
  if [ -n "$prior_rpc$prior_secret" ] && [ "$NONINTERACTIVE" = 0 ] \
     && [ -z "$HOST_ARG$PORT_ARG$RPC_URL_ARG$SECRET_ARG" ]; then
    if ask_yes "Existing configuration found in ${APP_DIR}/.env. Keep it?" y; then
      keep=1
    fi
  fi

  CONFIG_HOST="${HOST_ARG:-${prior_host:-$DEFAULT_HOST}}"
  CONFIG_PORT="${PORT_ARG:-${prior_port:-$DEFAULT_PORT}}"
  case "$CONFIG_PORT" in *[!0-9]*) CONFIG_PORT="$DEFAULT_PORT";; esac

  if [ "$keep" = 1 ]; then
    CONFIG_RPC="$prior_rpc"; CONFIG_SECRET="$prior_secret"
    return
  fi

  CONFIG_RPC="${RPC_URL_ARG:-${prior_rpc:-$DEFAULT_RPC_URL}}"
  detected="$(detect_rpc "$CONFIG_RPC")"

  # ── Non-interactive mode ─────────────────────────────────────
  if [ "$NONINTERACTIVE" = 1 ]; then
    CONFIG_SECRET="${SECRET_ARG:-$prior_secret}"
    case "$detected" in
      open)
        MODE="external"; CONFIG_SECRET=""
        ;;
      secret)
        MODE="external"
        if [ -z "$CONFIG_SECRET" ]; then
          warn "An RPC server at $CONFIG_RPC requires a secret token. Provide it with --rpc-secret."
        fi
        ;;
      none)
        CONFIG_RPC="$DEFAULT_RPC_URL"
        MODE="builtin"
        CONFIG_SECRET="${CONFIG_SECRET:-$(gen_secret)}"
        ;;
    esac
    return
  fi

  # ── Interactive: ask the user ────────────────────────────────
  if [ "$detected" = "none" ]; then
    info "No aria2 RPC server detected at $CONFIG_RPC."
    default_ons="n"
  else
    info "Detected an aria2 RPC server at $CONFIG_RPC${detected:+ (it uses a secret token)}."
    default_ons="y"
  fi

  if ask_yes "Is an aria2 RPC server already running?" "$default_ons"; then
    MODE="external"
    CONFIG_RPC="$(ask_str "RPC endpoint URL" "$CONFIG_RPC")"
    if [ "$detected" != "open" ] && [ -z "$SECRET_ARG" ]; then
      CONFIG_SECRET="$(ask_str "RPC secret token (leave empty if none)" "${prior_secret:-}")"
    else
      CONFIG_SECRET="${SECRET_ARG:-$prior_secret}"
    fi
  else
    MODE="builtin"
    CONFIG_RPC="$DEFAULT_RPC_URL"
    CONFIG_SECRET="$(gen_secret)"
    info "Using the app's built-in aria2 (it starts and supervises its own daemon)."
  fi

  if ask_yes "Expose the web UI beyond this machine (0.0.0.0)?" n; then
    CONFIG_HOST="${HOST_ARG:-0.0.0.0}"
  else
    CONFIG_HOST="${HOST_ARG:-127.0.0.1}"
  fi
}

# ── Write .env ─────────────────────────────────────────────────────

write_env() {
  {
    printf '# Generated by %s/setup.sh — edit and restart the service to apply.\n\n' "$APP_NAME"
    printf '# aria2 JSON-RPC endpoint. Empty ARIA2_SECRET with this default endpoint\n'
    printf '# means the web UI manages its own built-in aria2c daemon.\n'
    printf 'ARIA2_RPC=%s\n' "$CONFIG_RPC"
    printf 'ARIA2_SECRET=%s\n' "$CONFIG_SECRET"
    printf 'ARIA2_PORT=6800\n\n'
    printf '# Task persistence file\n'
    printf 'DB_FILE=%s\n\n' "${APP_DIR}/aria_tasks.json"
    printf '# Seconds before aborting a stalled download\n'
    printf 'DOWNLOAD_STALL_SECONDS=300\n\n'
    printf '# Web UI listen address and port\n'
    printf 'HOST=%s\n' "$CONFIG_HOST"
    printf 'PORT=%s\n' "$CONFIG_PORT"
  } > "$APP_DIR/.env"
  chmod 600 "$APP_DIR/.env"
  ok "Wrote configuration to $APP_DIR/.env."
}

# ── Service installation ───────────────────────────────────────────

install_service() {
  if [ "$DISTRO" = "opkg" ]; then
    cat > "/etc/init.d/${SERVICE_NAME}" <<'EOF'
#!/bin/sh /etc/rc.common
START=99
STOP=10
USE_PROCD=1

start_service() {
  procd_open_instance
  procd_set_param command PYTHON_BIN
  procd_append_param command /opt/aria2-webui/aria2-webui.py
  procd_set_param env PYTHONUNBUFFERED=1
  procd_set_param respawn
  procd_set_param stdout 1
  procd_set_param stderr 1
  procd_close_instance
}
EOF
    sed -i "s|PYTHON_BIN|${PYTHON_BIN}|" "/etc/init.d/${SERVICE_NAME}"
    chmod +x "/etc/init.d/${SERVICE_NAME}"
    ok "Wrote OpenWrt init script /etc/init.d/${SERVICE_NAME}."
    return
  fi

  cat > "/etc/systemd/system/${SERVICE_NAME}.service" <<EOF
[Unit]
Description=Aria2 Web UI (scheduling download manager)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${APP_DIR}
ExecStart=${PYTHON_BIN} ${APP_DIR}/aria2-webui.py

# Automatically restart if it crashes
Restart=always
RestartSec=3

# Environment variables (helps with path expansion)
Environment=PYTHONUNBUFFERED=1
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
EOF
  ok "Wrote systemd unit /etc/systemd/system/${SERVICE_NAME}.service."
}

# ── Start + verify ─────────────────────────────────────────────────

start_and_verify() {
  if [ "$DISTRO" = "opkg" ]; then
    "/etc/init.d/${SERVICE_NAME}" enable
    "/etc/init.d/${SERVICE_NAME}" start
  else
    systemctl daemon-reload
    systemctl enable --now "$SERVICE_NAME"
  fi

  info "Waiting for the web UI on port $CONFIG_PORT…"
  local i
  for i in $(seq 1 30); do
    if http_ok "http://127.0.0.1:${CONFIG_PORT}/health"; then
      ok "Web UI is responding on port $CONFIG_PORT."
      return 0
    fi
    sleep 1
  done
  warn "The web UI did not answer on port $CONFIG_PORT yet — check the service logs below."
}

# ── Summary ────────────────────────────────────────────────────────

web_url() {
  local ip
  if [ "$CONFIG_HOST" = "0.0.0.0" ]; then
    ip="$(hostname -I 2>/dev/null | awk '{print $1}')"
    [ -n "$ip" ] || ip="<this-machine-ip>"
    printf 'http://%s:%s' "$ip" "$CONFIG_PORT"
  else
    printf 'http://127.0.0.1:%s' "$CONFIG_PORT"
  fi
}

download_mode() {
  if [ "$MODE" = "external" ]; then
    if [ -n "$CONFIG_SECRET" ]; then
      printf 'using existing RPC at %s (secret set)' "$CONFIG_RPC"
    else
      printf 'using existing RPC at %s (no secret)' "$CONFIG_RPC"
    fi
  else
    printf 'managed by the app (built-in aria2c, %s)' \
      "$([ -n "$CONFIG_SECRET" ] && echo "secret set" || echo "no secret")"
  fi
}

summary() {
  local status_cmd log_cmd
  if [ "$DISTRO" = "opkg" ]; then
    status_cmd="/etc/init.d/${SERVICE_NAME} status"
    log_cmd="logread | grep aria2-webui"
  else
    status_cmd="systemctl status ${SERVICE_NAME}"
    log_cmd="journalctl -u ${SERVICE_NAME} -f"
  fi

  printf '\n'
  ok "aria2-webui is installed and running."
  printf '\n'
  info "Web UI:    %s" "$(web_url)"
  info "Downloads: %s" "$(download_mode)"
  printf '\n'
  printf '  Check status:  %s\n' "$status_cmd"
  printf '  Live logs:     %s\n' "$log_cmd"
  if [ "$DISTRO" = "opkg" ]; then
    printf '  Restart:       /etc/init.d/%s restart\n' "$SERVICE_NAME"
  else
    printf '  Restart:       systemctl restart %s\n' "$SERVICE_NAME"
  fi
  printf '  Uninstall:     sudo bash %s --uninstall\n' "$0"
  printf '\n'
  if [ "$CONFIG_HOST" != "0.0.0.0" ]; then
    info 'To access it from other devices, re-run with "y" at the expose question or --host 0.0.0.0.'
  fi
}

# ── Uninstall ──────────────────────────────────────────────────────

do_uninstall() {
  info "Stopping and removing services…"
  if command -v systemctl >/dev/null 2>&1; then
    systemctl disable --now "$SERVICE_NAME" 2>/dev/null || true
    rm -f "/etc/systemd/system/${SERVICE_NAME}.service"
    systemctl daemon-reload || true
  fi
  if [ -e "/etc/init.d/${SERVICE_NAME}" ]; then
    "/etc/init.d/${SERVICE_NAME}" stop 2>/dev/null || true
    "/etc/init.d/${SERVICE_NAME}" disable 2>/dev/null || true
    rm -f "/etc/init.d/${SERVICE_NAME}"
  fi
  rm -rf "$APP_DIR"
  ok "Uninstalled ${APP_NAME}. Installed packages (aria2, python3, curl) were left untouched."
}

# ── Main ───────────────────────────────────────────────────────────

main() {
  if [ "$UNINSTALL" = 1 ]; then
    do_uninstall
    return
  fi
  detect_distro
  install_packages
  install_into_opt
  decide_config
  write_env
  install_service
  start_and_verify
  summary
}

main