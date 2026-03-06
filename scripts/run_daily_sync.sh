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

ASR_BATCH="${SUBSTUDY_ASR_BATCH:-10}"
LOUDNESS_BATCH="${SUBSTUDY_LOUDNESS_BATCH:-80}"
CPU_SLEEP="${SUBSTUDY_CPU_SLEEP:-20}"

TRANSLATE_TARGET_LANG="${SUBSTUDY_TRANSLATE_TARGET_LANG:-ja-local}"
TRANSLATE_BATCH="${SUBSTUDY_TRANSLATE_BATCH:-1}"
TRANSLATE_FINAL_LIMIT="${SUBSTUDY_TRANSLATE_FINAL_LIMIT:-50}"
TRANSLATE_TIMEOUT="${SUBSTUDY_TRANSLATE_TIMEOUT:-300}"
MEM_SLEEP="${SUBSTUDY_MEM_SLEEP:-15}"
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

echo "[daily] 1) sync start (network lane)"
run_substudy sync \
  --skip-ledger \
  --config "${CONFIG_PATH}" \
  --ledger-db "${LEDGER_DB}" \
  "${METERED_MEDIA_ARGS[@]}" \
  "${NETWORK_ARGS[@]}" &
SYNC_PID=$!
echo "[daily] sync pid=${SYNC_PID}"

cpu_lane() {
  echo "[daily] 2-A) CPU lane start (asr/loudness)"
  while kill -0 "${SYNC_PID}" 2>/dev/null; do
    run_substudy asr \
      --config "${CONFIG_PATH}" \
      --ledger-db "${LEDGER_DB}" \
      --max-per-source "${ASR_BATCH}" || true
    run_substudy loudness \
      --config "${CONFIG_PATH}" \
      --ledger-db "${LEDGER_DB}" \
      --limit "${LOUDNESS_BATCH}" || true
    sleep "${CPU_SLEEP}"
  done
  echo "[daily] 2-A) CPU lane done"
}

mem_lane() {
  echo "[daily] 2-B) MEM lane start (translate-local)"
  while kill -0 "${SYNC_PID}" 2>/dev/null; do
    out="$(run_substudy translate-local \
      --config "${CONFIG_PATH}" \
      --ledger-db "${LEDGER_DB}" \
      --target-lang "${TRANSLATE_TARGET_LANG}" \
      --limit "${TRANSLATE_BATCH}" \
      --timeout-sec "${TRANSLATE_TIMEOUT}" 2>&1 || true)"
    printf '%s\n' "${out}"
    if [[ "${out}" == *"database is locked"* ]]; then
      sleep $((MEM_SLEEP * 3))
    else
      sleep "${MEM_SLEEP}"
    fi
  done
  echo "[daily] 2-B) MEM lane done"
}

cleanup() {
  for pid in "${SYNC_PID:-}" "${CPU_PID:-}" "${MEM_PID:-}"; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" 2>/dev/null || true
    fi
  done
  wait || true
}
trap cleanup INT TERM

cpu_lane &
CPU_PID=$!
mem_lane &
MEM_PID=$!

echo "[daily] 3) wait sync"
wait "${SYNC_PID}"

echo "[daily] 4) wait workers"
wait "${CPU_PID}" || true
wait "${MEM_PID}" || true

echo "[daily] 5) backfill after sync"
run_substudy backfill \
  --skip-ledger \
  --config "${CONFIG_PATH}" \
  --ledger-db "${LEDGER_DB}" \
  "${METERED_MEDIA_ARGS[@]}" \
  "${NETWORK_ARGS[@]}" || true

echo "[daily] 6) final catch-up"
run_substudy asr --config "${CONFIG_PATH}" --ledger-db "${LEDGER_DB}" || true
run_substudy loudness --config "${CONFIG_PATH}" --ledger-db "${LEDGER_DB}" || true
run_substudy translate-local \
  --config "${CONFIG_PATH}" \
  --ledger-db "${LEDGER_DB}" \
  --target-lang "${TRANSLATE_TARGET_LANG}" \
  --limit "${TRANSLATE_FINAL_LIMIT}" \
  --timeout-sec "${TRANSLATE_TIMEOUT}" || true

echo "[daily] 7) ledger + downloads report"
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
