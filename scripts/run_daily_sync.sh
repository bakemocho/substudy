#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"
export PATH="/opt/homebrew/bin:/usr/local/bin:${PATH}"

resolve_python_bin() {
  local candidates=()
  local candidate=""
  if [[ -n "${SUBSTUDY_PYTHON:-}" ]]; then
    candidates+=("${SUBSTUDY_PYTHON}")
  fi
  candidates+=("/opt/homebrew/bin/python3" "/usr/local/bin/python3")
  if command -v python3 >/dev/null 2>&1; then
    candidates+=("$(command -v python3)")
  fi

  for candidate in "${candidates[@]}"; do
    [[ -x "${candidate}" ]] || continue
    if "${candidate}" - <<'PY' >/dev/null 2>&1
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
    then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

  echo "error: Python 3.11+ interpreter not found." >&2
  echo "Set SUBSTUDY_PYTHON to a compatible python executable." >&2
  return 1
}

PYTHON_BIN="$(resolve_python_bin)"

CONFIG_PATH="${SUBSTUDY_CONFIG_PATH:-${REPO_ROOT}/config/sources.toml}"
LEDGER_DB="${SUBSTUDY_LEDGER_DB:-${REPO_ROOT}/data/master_ledger.sqlite}"

SOURCE_ARGS=()
NETWORK_PROFILE="${SUBSTUDY_NETWORK_PROFILE:-auto}"
NETWORK_PROBE_URL="${SUBSTUDY_NETWORK_PROBE_URL:-https://www.tiktok.com/robots.txt}"
NETWORK_PROBE_TIMEOUT_SEC="${SUBSTUDY_NETWORK_PROBE_TIMEOUT_SEC:-8}"
NETWORK_PROBE_BYTES="${SUBSTUDY_NETWORK_PROBE_BYTES:-131072}"
WEAK_NET_MIN_KBPS="${SUBSTUDY_WEAK_NET_MIN_KBPS:-900}"
WEAK_NET_MAX_RTT_MS="${SUBSTUDY_WEAK_NET_MAX_RTT_MS:-900}"

NETWORK_ARGS=(
  --network-profile "${NETWORK_PROFILE}"
  --network-probe-url "${NETWORK_PROBE_URL}"
  --network-probe-timeout-sec "${NETWORK_PROBE_TIMEOUT_SEC}"
  --network-probe-bytes "${NETWORK_PROBE_BYTES}"
  --weak-net-min-kbps "${WEAK_NET_MIN_KBPS}"
  --weak-net-max-rtt-ms "${WEAK_NET_MAX_RTT_MS}"
)

METERED_LINK_MODE="${SUBSTUDY_METERED_LINK_MODE:-auto}"
METERED_MIN_ARCHIVE_IDS="${SUBSTUDY_METERED_MIN_ARCHIVE_IDS:-200}"
METERED_PLAYLIST_END="${SUBSTUDY_METERED_PLAYLIST_END:-40}"
QUEUE_WORKER_LEASE_SEC="${SUBSTUDY_QUEUE_WORKER_LEASE_SEC:-1800}"
QUEUE_WORKER_POLL_SEC="${SUBSTUDY_QUEUE_WORKER_POLL_SEC:-2.0}"
QUEUE_WORKER_MAX_ATTEMPTS="${SUBSTUDY_QUEUE_WORKER_MAX_ATTEMPTS:-8}"
MEDIA_WORKER_ENABLED="${SUBSTUDY_MEDIA_WORKER_ENABLED:-1}"
PIPELINE_WORKER_ENABLED="${SUBSTUDY_PIPELINE_WORKER_ENABLED:-1}"
MEDIA_WORKER_MAX_ITEMS="${SUBSTUDY_MEDIA_WORKER_MAX_ITEMS:-0}"
PIPELINE_WORKER_MAX_ITEMS="${SUBSTUDY_PIPELINE_WORKER_MAX_ITEMS:-0}"
QUEUE_DRAIN_TIMEOUT_SEC="${SUBSTUDY_QUEUE_DRAIN_TIMEOUT_SEC:-600}"
QUEUE_DRAIN_POLL_SEC="${SUBSTUDY_QUEUE_DRAIN_POLL_SEC:-5}"
QUEUE_RECOVER_KNOWN_ENABLED="${SUBSTUDY_QUEUE_RECOVER_KNOWN_ENABLED:-1}"
QUEUE_RECOVER_KNOWN_PROFILES="${SUBSTUDY_QUEUE_RECOVER_KNOWN_PROFILES:-all}"
QUEUE_RECOVER_KNOWN_LIMIT="${SUBSTUDY_QUEUE_RECOVER_KNOWN_LIMIT:-0}"
YTDLP_UPDATE_MODE="${SUBSTUDY_YTDLP_UPDATE_MODE:-auto}"
YTDLP_UV_WITH_CURL_CFFI="${SUBSTUDY_YTDLP_UV_WITH_CURL_CFFI:-1}"
YTDLP_UPDATE_INTERVAL_SEC="${SUBSTUDY_YTDLP_UPDATE_INTERVAL_SEC:-86400}"
YTDLP_UPDATE_STATE_FILE="${SUBSTUDY_YTDLP_UPDATE_STATE_FILE:-${REPO_ROOT}/data/runtime/yt_dlp_update_state}"
YTDLP_UPDATE_LOCK_DIR="${SUBSTUDY_YTDLP_UPDATE_LOCK_DIR:-${REPO_ROOT}/data/locks/ytdlp_update.lock}"
YTDLP_REQUIRE_LATEST="${SUBSTUDY_YTDLP_REQUIRE_LATEST:-0}"
YTDLP_REQUIRE_ARGS=()
if [[ "${YTDLP_REQUIRE_LATEST}" != "0" ]]; then
  YTDLP_REQUIRE_ARGS=(
    --require-current-ytdlp
    --ytdlp-check-mode "${YTDLP_UPDATE_MODE}"
  )
fi
while (($# > 0)); do
  case "$1" in
    --source)
      if (($# < 2)); then
        echo "error: --source requires a value" >&2
        exit 1
      fi
      SOURCE_ARGS+=("$1" "$2")
      shift 2
      ;;
    *)
      echo "error: unsupported arg '$1' (only --source <id> is supported)" >&2
      exit 1
      ;;
  esac
done

run_substudy() {
  local command="$1"
  shift
  local -a cmd=(
    "${PYTHON_BIN}"
    "${REPO_ROOT}/scripts/substudy.py"
    "${command}"
    "$@"
  )
  if ((${#SOURCE_ARGS[@]} > 0)); then
    cmd+=("${SOURCE_ARGS[@]}")
  fi
  "${cmd[@]}"
}

resolve_configured_ytdlp_bin() {
  "${PYTHON_BIN}" - "${CONFIG_PATH}" <<'PY'
from pathlib import Path
import sys
import tomllib

config_path = Path(sys.argv[1])
try:
    with config_path.open("rb") as fh:
        config = tomllib.load(fh)
except Exception:
    print("")
    raise SystemExit(0)

value = config.get("global", {}).get("ytdlp_bin", "")
print(value if isinstance(value, str) else "")
PY
}

resolve_effective_ytdlp_bin() {
  local configured=""
  configured="$(resolve_configured_ytdlp_bin)"
  if [[ -n "${configured}" ]]; then
    if [[ "${configured}" == */* ]]; then
      if [[ -x "${configured}" ]]; then
        printf '%s\n' "${configured}"
        return 0
      fi
    elif command -v "${configured}" >/dev/null 2>&1; then
      command -v "${configured}"
      return 0
    fi
  fi
  if command -v yt-dlp >/dev/null 2>&1; then
    command -v yt-dlp
    return 0
  fi
  printf '%s\n' ""
}

run_ytdlp_update() {
  local mode="$1"
  local runtime_bin="$2"
  local use_uv="0"

  case "${mode}" in
    off)
      echo "[daily] yt-dlp update skipped (mode=off)"
      return 0
      ;;
    auto|uv|brew)
      ;;
    *)
      echo "[daily] warning: unknown SUBSTUDY_YTDLP_UPDATE_MODE='${mode}', fallback to auto" >&2
      mode="auto"
      ;;
  esac

  if [[ "${mode}" == "uv" ]]; then
    use_uv="1"
  elif [[ "${mode}" == "auto" && "${runtime_bin}" == "${HOME}/.local/bin/yt-dlp" ]]; then
    use_uv="1"
  fi

  if [[ "${use_uv}" == "1" ]]; then
    if command -v uv >/dev/null 2>&1; then
      if [[ "${YTDLP_UV_WITH_CURL_CFFI}" == "0" ]]; then
        echo "[daily] uv tool install yt-dlp --force"
        if uv tool install yt-dlp --force; then
          echo "[daily] yt-dlp upgraded via uv"
        else
          echo "[daily] warning: uv yt-dlp upgrade failed; continuing" >&2
        fi
      else
        echo "[daily] uv tool install yt-dlp --with curl-cffi --force"
        if uv tool install yt-dlp --with curl-cffi --force; then
          echo "[daily] yt-dlp upgraded via uv (+curl-cffi)"
        else
          echo "[daily] warning: uv yt-dlp (+curl-cffi) upgrade failed; continuing" >&2
        fi
      fi
    else
      echo "[daily] warning: uv not found; skip uv yt-dlp upgrade" >&2
    fi
    return 0
  fi

  if command -v brew >/dev/null 2>&1; then
    if brew list --formula yt-dlp >/dev/null 2>&1; then
      echo "[daily] brew upgrade yt-dlp"
      if brew upgrade yt-dlp; then
        echo "[daily] yt-dlp upgraded via brew"
      else
        echo "[daily] warning: brew yt-dlp upgrade failed; continuing" >&2
      fi
    else
      echo "[daily] warning: yt-dlp is not a Homebrew formula; skip brew upgrade" >&2
    fi
  else
    echo "[daily] warning: Homebrew not found; skip brew yt-dlp upgrade" >&2
  fi
}

read_ytdlp_last_update_epoch() {
  if [[ -f "${YTDLP_UPDATE_STATE_FILE}" ]]; then
    cat "${YTDLP_UPDATE_STATE_FILE}" 2>/dev/null || true
  fi
}

write_ytdlp_last_update_epoch() {
  local now_epoch="$1"
  mkdir -p "$(dirname "${YTDLP_UPDATE_STATE_FILE}")"
  printf '%s\n' "${now_epoch}" > "${YTDLP_UPDATE_STATE_FILE}"
}

is_ytdlp_update_due() {
  local interval="$1"
  local now_epoch="$2"
  local last_epoch=""
  local elapsed=""

  if [[ "${interval}" -le 0 ]]; then
    return 0
  fi
  last_epoch="$(read_ytdlp_last_update_epoch)"
  if ! [[ "${last_epoch}" =~ ^[0-9]+$ ]]; then
    return 0
  fi
  elapsed=$((now_epoch - last_epoch))
  if ((elapsed < interval)); then
    return 1
  fi
  return 0
}

run_daily_ytdlp_update_guarded() {
  local now_epoch=""
  local effective_bin=""
  local rc=0

  now_epoch="$(date +%s)"
  if ! is_ytdlp_update_due "${YTDLP_UPDATE_INTERVAL_SEC}" "${now_epoch}"; then
    echo "[daily] yt-dlp update skipped (cooldown active: ${YTDLP_UPDATE_INTERVAL_SEC}s)"
    return 0
  fi

  mkdir -p "$(dirname "${YTDLP_UPDATE_LOCK_DIR}")"
  if ! mkdir "${YTDLP_UPDATE_LOCK_DIR}" 2>/dev/null; then
    echo "[daily] yt-dlp update skipped (another updater is running)"
    return 0
  fi
  {
    now_epoch="$(date +%s)"
    if ! is_ytdlp_update_due "${YTDLP_UPDATE_INTERVAL_SEC}" "${now_epoch}"; then
      echo "[daily] yt-dlp update skipped after lock (cooldown active)"
      return 0
    fi

    effective_bin="$(resolve_effective_ytdlp_bin)"
    echo "[daily] yt-dlp target=${effective_bin:-not-found} update-mode=${YTDLP_UPDATE_MODE} interval=${YTDLP_UPDATE_INTERVAL_SEC}s"
    local -a ytdlp_update_args=(
      ytdlp-update
      --config "${CONFIG_PATH}"
      --ledger-db "${LEDGER_DB}"
      --mode "${YTDLP_UPDATE_MODE}"
      --trigger daily
    )
    if [[ "${YTDLP_UV_WITH_CURL_CFFI}" == "0" ]]; then
      ytdlp_update_args+=(--no-uv-with-curl-cffi)
    else
      ytdlp_update_args+=(--uv-with-curl-cffi)
    fi
    run_substudy "${ytdlp_update_args[@]}" || true
    write_ytdlp_last_update_epoch "${now_epoch}"
  } || rc=$?
  rm -rf "${YTDLP_UPDATE_LOCK_DIR}" 2>/dev/null || true
  return "${rc}"
}

run_daily_ytdlp_latest_preflight() {
  if [[ "${YTDLP_REQUIRE_LATEST}" == "0" ]]; then
    return 0
  fi

  echo "[daily] yt-dlp latest preflight enabled"
  local -a ytdlp_check_args=(
    ytdlp-check
    --config "${CONFIG_PATH}"
    --ledger-db "${LEDGER_DB}"
    --mode "${YTDLP_UPDATE_MODE}"
    --trigger daily
    --fail-if-outdated
  )
  run_substudy "${ytdlp_check_args[@]}"
}

route_default_field() {
  local field="$1"
  route -n get default 2>/dev/null | awk -v key="${field}" '$1 == key {print $2; exit}'
}

detect_hardware_port_for_device() {
  local device="$1"
  [[ -n "${device}" ]] || return 0
  networksetup -listallhardwareports 2>/dev/null | awk -v target="${device}" '
    /^Hardware Port: / {port = substr($0, 16)}
    /^Device: / {
      dev = substr($0, 9)
      if (dev == target) {
        print port
        exit
      }
    }
  '
}

detect_wifi_ssid_for_device() {
  local device="$1"
  local output=""
  [[ -n "${device}" ]] || return 0
  output="$(networksetup -getairportnetwork "${device}" 2>/dev/null || true)"
  if [[ "${output}" == Current\ Wi-Fi\ Network:* ]]; then
    printf '%s\n' "${output#Current Wi-Fi Network: }"
    return 0
  fi
  printf '%s\n' ""
}

detect_interface_is_expensive() {
  local device="$1"
  local summary=""
  [[ -n "${device}" ]] || {
    printf '%s\n' "0"
    return 0
  }
  summary="$(ipconfig getsummary "${device}" 2>/dev/null || true)"
  if [[ "${summary}" == *"IsExpensive : TRUE"* ]]; then
    printf '%s\n' "1"
    return 0
  fi
  printf '%s\n' "0"
}

detect_metered_link() {
  local mode="$1"
  local default_if=""
  local gateway=""
  local hw_port=""
  local ssid=""
  local ssid_lower=""
  local is_expensive="0"
  local is_metered="0"
  local reason="auto: non-metered"

  default_if="$(route_default_field "interface:")"
  gateway="$(route_default_field "gateway:")"
  hw_port="$(detect_hardware_port_for_device "${default_if}")"
  ssid="$(detect_wifi_ssid_for_device "${default_if}")"
  ssid_lower="$(printf '%s' "${ssid}" | tr '[:upper:]' '[:lower:]')"
  is_expensive="$(detect_interface_is_expensive "${default_if}")"

  case "${mode}" in
    on)
      is_metered="1"
      reason="manual mode=on"
      ;;
    off)
      is_metered="0"
      reason="manual mode=off"
      ;;
    *)
      if [[ "${default_if}" == pdp_ip* ]]; then
        is_metered="1"
        reason="auto: cellular interface (${default_if})"
      elif [[ "${hw_port}" == "iPhone USB" ]]; then
        is_metered="1"
        reason="auto: iPhone USB tethering"
      elif [[ "${hw_port}" == "Bluetooth PAN" ]]; then
        is_metered="1"
        reason="auto: Bluetooth PAN tethering"
      elif [[ "${gateway}" == "172.20.10.1" ]]; then
        is_metered="1"
        reason="auto: hotspot gateway (${gateway})"
      elif [[ "${is_expensive}" == "1" ]]; then
        is_metered="1"
        reason="auto: interface marked expensive (${default_if})"
      elif [[ -n "${ssid_lower}" ]] && (
        [[ "${ssid_lower}" == *"iphone"* ]] ||
        [[ "${ssid_lower}" == *"android"* ]] ||
        [[ "${ssid_lower}" == *"pixel"* ]] ||
        [[ "${ssid_lower}" == *"galaxy"* ]] ||
        [[ "${ssid_lower}" == *"xperia"* ]] ||
        [[ "${ssid_lower}" == *"hotspot"* ]] ||
        [[ "${ssid_lower}" == *"tether"* ]]
      ); then
        is_metered="1"
        reason="auto: hotspot-like ssid (${ssid})"
      fi
      ;;
  esac

  printf '%s|%s|%s|%s|%s|%s|%s\n' \
    "${is_metered}" \
    "${reason}" \
    "${default_if}" \
    "${gateway}" \
    "${hw_port}" \
    "${ssid}" \
    "${is_expensive}"
}

echo "[daily] start: $(date '+%Y-%m-%d %H:%M:%S')"
metered_info="$(detect_metered_link "${METERED_LINK_MODE}")"
IFS='|' read -r IS_METERED_LINK METERED_REASON DEFAULT_IFACE DEFAULT_GATEWAY DEFAULT_HW_PORT DEFAULT_WIFI_SSID DEFAULT_IS_EXPENSIVE <<< "${metered_info}"
echo "[daily] link mode=${METERED_LINK_MODE} metered=${IS_METERED_LINK} reason=${METERED_REASON}"
echo "[daily] route iface=${DEFAULT_IFACE:-unknown} gateway=${DEFAULT_GATEWAY:-unknown} hw_port=${DEFAULT_HW_PORT:-unknown} ssid=${DEFAULT_WIFI_SSID:-unknown} expensive=${DEFAULT_IS_EXPENSIVE:-0}"

METERED_MEDIA_ARGS=(--metered-media-mode off)
if [[ "${IS_METERED_LINK}" == "1" ]]; then
  METERED_MEDIA_ARGS=(
    --metered-media-mode updates-only
    --metered-min-archive-ids "${METERED_MIN_ARCHIVE_IDS}"
    --metered-playlist-end "${METERED_PLAYLIST_END}"
  )
fi

run_daily_ytdlp_update_guarded
run_daily_ytdlp_latest_preflight

SYNC_PID=""
declare -a WORKER_PIDS=()

start_worker() {
  local label="$1"
  shift
  run_substudy queue-worker \
    --config "${CONFIG_PATH}" \
    --ledger-db "${LEDGER_DB}" \
    --lease-sec "${QUEUE_WORKER_LEASE_SEC}" \
    --poll-sec "${QUEUE_WORKER_POLL_SEC}" \
    --max-attempts "${QUEUE_WORKER_MAX_ATTEMPTS}" \
    "$@" &
  local pid=$!
  WORKER_PIDS+=("${pid}")
  echo "[daily] worker ${label} pid=${pid}"
}

stop_workers() {
  local pid=""
  for pid in "${WORKER_PIDS[@]:-}"; do
    [[ -n "${pid}" ]] || continue
    if kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" 2>/dev/null || true
    fi
  done
  for pid in "${WORKER_PIDS[@]:-}"; do
    [[ -n "${pid}" ]] || continue
    wait "${pid}" || true
  done
  WORKER_PIDS=()
}

queue_pending_count() {
  "${PYTHON_BIN}" - "${LEDGER_DB}" <<'PY'
import datetime as dt
import sqlite3
import sys

db_path = sys.argv[1]
now_iso = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
try:
    connection = sqlite3.connect(db_path, timeout=5)
except Exception:
    print(0)
    raise SystemExit(0)

try:
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM work_items
        WHERE stage != 'translate'
          AND (
             status IN ('queued', 'leased')
           OR (
             status = 'error'
             AND next_retry_at IS NOT NULL
             AND next_retry_at <= ?
           ))
        """,
        (now_iso,),
    ).fetchone()
    print(int(row[0] if row else 0))
except Exception:
    print(0)
finally:
    connection.close()
PY
}

wait_for_queue_drain() {
  local timeout_sec="$1"
  local poll_sec="$2"
  local deadline=$((SECONDS + timeout_sec))
  local pending="0"
  while ((SECONDS < deadline)); do
    pending="$(queue_pending_count)"
    echo "[daily] queue pending=${pending}"
    if [[ "${pending}" == "0" ]]; then
      return 0
    fi
    sleep "${poll_sec}"
  done
  echo "[daily] warning: queue drain timeout pending=${pending}" >&2
  return 1
}

cleanup() {
  if [[ -n "${SYNC_PID}" ]] && kill -0 "${SYNC_PID}" 2>/dev/null; then
    kill "${SYNC_PID}" 2>/dev/null || true
  fi
  stop_workers
  wait || true
}
trap cleanup INT TERM

echo "[daily] 1) start queue producer (sync)"
run_substudy sync \
  --execution-mode queue \
  --skip-ledger \
  --config "${CONFIG_PATH}" \
  --ledger-db "${LEDGER_DB}" \
  "${YTDLP_REQUIRE_ARGS[@]}" \
  "${METERED_MEDIA_ARGS[@]}" \
  "${NETWORK_ARGS[@]}" &
SYNC_PID=$!
echo "[daily] sync producer pid=${SYNC_PID}"

echo "[daily] 2) start queue workers"
if [[ "${MEDIA_WORKER_ENABLED}" != "0" ]]; then
  start_worker "media" \
    --stage media \
    --max-items "${MEDIA_WORKER_MAX_ITEMS}"
fi
if [[ "${PIPELINE_WORKER_ENABLED}" != "0" ]]; then
  start_worker "pipeline" \
    --stage subs \
    --stage meta \
    --stage asr \
    --stage loudness \
    --no-enqueue-downstream \
    --max-items "${PIPELINE_WORKER_MAX_ITEMS}"
fi

echo "[daily] 3) wait sync producer"
wait "${SYNC_PID}"
SYNC_PID=""

echo "[daily] 4) run queue producer (backfill)"
run_substudy backfill \
  --execution-mode queue \
  --skip-ledger \
  --config "${CONFIG_PATH}" \
  --ledger-db "${LEDGER_DB}" \
  "${YTDLP_REQUIRE_ARGS[@]}" \
  "${METERED_MEDIA_ARGS[@]}" \
  "${NETWORK_ARGS[@]}" || true

echo "[daily] 5) run queue known recovery"
if [[ "${QUEUE_RECOVER_KNOWN_ENABLED}" != "0" ]]; then
  RECOVER_PROFILE_ARGS=()
  IFS=',' read -r -a _recover_profiles <<< "${QUEUE_RECOVER_KNOWN_PROFILES}"
  for _raw_profile in "${_recover_profiles[@]}"; do
    _profile="${_raw_profile//[[:space:]]/}"
    [[ -n "${_profile}" ]] || continue
    RECOVER_PROFILE_ARGS+=(--profile "${_profile}")
  done
  run_substudy queue-recover-known \
    --config "${CONFIG_PATH}" \
    --ledger-db "${LEDGER_DB}" \
    --limit "${QUEUE_RECOVER_KNOWN_LIMIT}" \
    "${RECOVER_PROFILE_ARGS[@]}" || true
else
  echo "[daily] queue known recovery skipped (SUBSTUDY_QUEUE_RECOVER_KNOWN_ENABLED=0)"
fi

echo "[daily] 6) wait queue drain"
wait_for_queue_drain "${QUEUE_DRAIN_TIMEOUT_SEC}" "${QUEUE_DRAIN_POLL_SEC}" || true

echo "[daily] 7) stop workers"
stop_workers

echo "[daily] 8) ledger + downloads report"
run_substudy ledger \
  --incremental \
  --config "${CONFIG_PATH}" \
  --ledger-db "${LEDGER_DB}"
run_substudy downloads \
  --config "${CONFIG_PATH}" \
  --ledger-db "${LEDGER_DB}" \
  --since-hours 24 \
  --limit 50 || true

echo "[daily] done: $(date '+%Y-%m-%d %H:%M:%S')"
