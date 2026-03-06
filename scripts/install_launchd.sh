#!/usr/bin/env bash
set -euo pipefail

if [[ "${OSTYPE:-}" != darwin* ]]; then
  echo "This installer supports macOS launchd only."
  exit 1
fi

DAILY_HOUR="${1:-6}"
DAILY_MINUTE="${2:-30}"
WEEKLY_WEEKDAY="${3:-0}"   # 0 or 7 = Sunday
WEEKLY_HOUR="${4:-7}"
WEEKLY_MINUTE="${5:-0}"
LABEL_PREFIX="${6:-com.substudy}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${REPO_ROOT}/logs"
AGENT_DIR="${HOME}/Library/LaunchAgents"

CONFIG_PATH="${SUBSTUDY_CONFIG_PATH:-${REPO_ROOT}/config/sources.toml}"
LEDGER_DB="${SUBSTUDY_LEDGER_DB:-${REPO_ROOT}/data/master_ledger.sqlite}"
PYTHON_BIN="${SUBSTUDY_PYTHON:-$(command -v python3 || true)}"

ENABLE_WORKER_JOBS="${SUBSTUDY_ENABLE_WORKER_JOBS:-1}"
MEDIA_WORKER_INTERVAL_SEC="${SUBSTUDY_MEDIA_WORKER_INTERVAL_SEC:-300}"
PIPELINE_WORKER_INTERVAL_SEC="${SUBSTUDY_PIPELINE_WORKER_INTERVAL_SEC:-300}"
QUEUE_WORKER_LEASE_SEC="${SUBSTUDY_QUEUE_WORKER_LEASE_SEC:-1800}"
QUEUE_WORKER_POLL_SEC="${SUBSTUDY_QUEUE_WORKER_POLL_SEC:-2.0}"
QUEUE_WORKER_MAX_ATTEMPTS="${SUBSTUDY_QUEUE_WORKER_MAX_ATTEMPTS:-8}"
MEDIA_WORKER_MAX_ITEMS="${SUBSTUDY_MEDIA_WORKER_MAX_ITEMS:-80}"
PIPELINE_WORKER_MAX_ITEMS="${SUBSTUDY_PIPELINE_WORKER_MAX_ITEMS:-120}"
TRANSLATE_TARGET_LANG="${SUBSTUDY_TRANSLATE_TARGET_LANG:-ja-local}"
TRANSLATE_SOURCE_TRACK="${SUBSTUDY_TRANSLATE_SOURCE_TRACK:-auto}"
TRANSLATE_TIMEOUT="${SUBSTUDY_TRANSLATE_TIMEOUT:-300}"

DAILY_LABEL="${LABEL_PREFIX}.daily"
WEEKLY_LABEL="${LABEL_PREFIX}.weekly_full"
MEDIA_WORKER_LABEL="${LABEL_PREFIX}.worker_media"
PIPELINE_WORKER_LABEL="${LABEL_PREFIX}.worker_pipeline"

DAILY_PLIST="${AGENT_DIR}/${DAILY_LABEL}.plist"
WEEKLY_PLIST="${AGENT_DIR}/${WEEKLY_LABEL}.plist"
MEDIA_WORKER_PLIST="${AGENT_DIR}/${MEDIA_WORKER_LABEL}.plist"
PIPELINE_WORKER_PLIST="${AGENT_DIR}/${PIPELINE_WORKER_LABEL}.plist"

DAILY_SCRIPT="${REPO_ROOT}/scripts/run_daily_sync.sh"
WEEKLY_SCRIPT="${REPO_ROOT}/scripts/run_weekly_full_sync.sh"
SUBSTUDY_SCRIPT="${REPO_ROOT}/scripts/substudy.py"
OLD_PLIST="${AGENT_DIR}/${LABEL_PREFIX}.sync.plist"

if [[ -z "${PYTHON_BIN}" || ! -x "${PYTHON_BIN}" ]]; then
  echo "error: python3 not found. Set SUBSTUDY_PYTHON to a valid executable." >&2
  exit 1
fi

mkdir -p "${AGENT_DIR}"
mkdir -p "${LOG_DIR}"

write_calendar_script_plist() {
  local plist_path="$1"
  local label="$2"
  local run_script="$3"
  local hour="$4"
  local minute="$5"
  local weekday="${6:-}"

  cat > "${plist_path}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${label}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${run_script}</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>SUBSTUDY_MEDIA_WORKER_ENABLED</key>
    <string>0</string>
    <key>SUBSTUDY_PIPELINE_WORKER_ENABLED</key>
    <string>0</string>
  </dict>
  <key>WorkingDirectory</key>
  <string>${REPO_ROOT}</string>
  <key>RunAtLoad</key>
  <false/>
  <key>StartCalendarInterval</key>
  <dict>
EOF

  if [[ -n "${weekday}" ]]; then
    cat >> "${plist_path}" <<EOF
    <key>Weekday</key>
    <integer>${weekday}</integer>
EOF
  fi

  cat >> "${plist_path}" <<EOF
    <key>Hour</key>
    <integer>${hour}</integer>
    <key>Minute</key>
    <integer>${minute}</integer>
  </dict>
  <key>StandardOutPath</key>
  <string>${LOG_DIR}/${label}.out.log</string>
  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/${label}.err.log</string>
</dict>
</plist>
EOF
}

write_media_worker_plist() {
  local plist_path="$1"
  local label="$2"
  local interval_sec="$3"

  cat > "${plist_path}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${label}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${PYTHON_BIN}</string>
    <string>${SUBSTUDY_SCRIPT}</string>
    <string>queue-worker</string>
    <string>--config</string>
    <string>${CONFIG_PATH}</string>
    <string>--ledger-db</string>
    <string>${LEDGER_DB}</string>
    <string>--stage</string>
    <string>media</string>
    <string>--max-items</string>
    <string>${MEDIA_WORKER_MAX_ITEMS}</string>
    <string>--lease-sec</string>
    <string>${QUEUE_WORKER_LEASE_SEC}</string>
    <string>--poll-sec</string>
    <string>${QUEUE_WORKER_POLL_SEC}</string>
    <string>--max-attempts</string>
    <string>${QUEUE_WORKER_MAX_ATTEMPTS}</string>
  </array>
  <key>WorkingDirectory</key>
  <string>${REPO_ROOT}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>StartInterval</key>
  <integer>${interval_sec}</integer>
  <key>StandardOutPath</key>
  <string>${LOG_DIR}/${label}.out.log</string>
  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/${label}.err.log</string>
</dict>
</plist>
EOF
}

write_pipeline_worker_plist() {
  local plist_path="$1"
  local label="$2"
  local interval_sec="$3"

  cat > "${plist_path}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${label}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${PYTHON_BIN}</string>
    <string>${SUBSTUDY_SCRIPT}</string>
    <string>queue-worker</string>
    <string>--config</string>
    <string>${CONFIG_PATH}</string>
    <string>--ledger-db</string>
    <string>${LEDGER_DB}</string>
    <string>--stage</string>
    <string>subs</string>
    <string>--stage</string>
    <string>meta</string>
    <string>--stage</string>
    <string>asr</string>
    <string>--stage</string>
    <string>loudness</string>
    <string>--stage</string>
    <string>translate</string>
    <string>--translate-target-lang</string>
    <string>${TRANSLATE_TARGET_LANG}</string>
    <string>--translate-source-track</string>
    <string>${TRANSLATE_SOURCE_TRACK}</string>
    <string>--translate-timeout-sec</string>
    <string>${TRANSLATE_TIMEOUT}</string>
    <string>--max-items</string>
    <string>${PIPELINE_WORKER_MAX_ITEMS}</string>
    <string>--lease-sec</string>
    <string>${QUEUE_WORKER_LEASE_SEC}</string>
    <string>--poll-sec</string>
    <string>${QUEUE_WORKER_POLL_SEC}</string>
    <string>--max-attempts</string>
    <string>${QUEUE_WORKER_MAX_ATTEMPTS}</string>
  </array>
  <key>WorkingDirectory</key>
  <string>${REPO_ROOT}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>StartInterval</key>
  <integer>${interval_sec}</integer>
  <key>StandardOutPath</key>
  <string>${LOG_DIR}/${label}.out.log</string>
  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/${label}.err.log</string>
</dict>
</plist>
EOF
}

install_job() {
  local label="$1"
  local plist_path="$2"

  launchctl bootout "gui/${UID}/${label}" >/dev/null 2>&1 || true
  launchctl bootstrap "gui/${UID}" "${plist_path}"
  launchctl enable "gui/${UID}/${label}"
}

remove_job() {
  local label="$1"
  local plist_path="$2"

  launchctl bootout "gui/${UID}/${label}" >/dev/null 2>&1 || true
  rm -f "${plist_path}"
}

# Clean up older single-job setup if present.
launchctl bootout "gui/${UID}/${LABEL_PREFIX}.sync" >/dev/null 2>&1 || true
rm -f "${OLD_PLIST}"

write_calendar_script_plist "${DAILY_PLIST}" "${DAILY_LABEL}" "${DAILY_SCRIPT}" "${DAILY_HOUR}" "${DAILY_MINUTE}"
write_calendar_script_plist "${WEEKLY_PLIST}" "${WEEKLY_LABEL}" "${WEEKLY_SCRIPT}" "${WEEKLY_HOUR}" "${WEEKLY_MINUTE}" "${WEEKLY_WEEKDAY}"
install_job "${DAILY_LABEL}" "${DAILY_PLIST}"
install_job "${WEEKLY_LABEL}" "${WEEKLY_PLIST}"

if [[ "${ENABLE_WORKER_JOBS}" != "0" ]]; then
  write_media_worker_plist "${MEDIA_WORKER_PLIST}" "${MEDIA_WORKER_LABEL}" "${MEDIA_WORKER_INTERVAL_SEC}"
  write_pipeline_worker_plist "${PIPELINE_WORKER_PLIST}" "${PIPELINE_WORKER_LABEL}" "${PIPELINE_WORKER_INTERVAL_SEC}"
  install_job "${MEDIA_WORKER_LABEL}" "${MEDIA_WORKER_PLIST}"
  install_job "${PIPELINE_WORKER_LABEL}" "${PIPELINE_WORKER_PLIST}"
else
  remove_job "${MEDIA_WORKER_LABEL}" "${MEDIA_WORKER_PLIST}"
  remove_job "${PIPELINE_WORKER_LABEL}" "${PIPELINE_WORKER_PLIST}"
fi

echo "Installed launchd jobs:"
echo "  ${DAILY_LABEL}: daily producer at ${DAILY_HOUR}:$(printf "%02d" "${DAILY_MINUTE}")"
echo "  ${WEEKLY_LABEL}: weekly producer weekday=${WEEKLY_WEEKDAY} at ${WEEKLY_HOUR}:$(printf "%02d" "${WEEKLY_MINUTE}")"
if [[ "${ENABLE_WORKER_JOBS}" != "0" ]]; then
  echo "  ${MEDIA_WORKER_LABEL}: media worker every ${MEDIA_WORKER_INTERVAL_SEC}s"
  echo "  ${PIPELINE_WORKER_LABEL}: pipeline worker every ${PIPELINE_WORKER_INTERVAL_SEC}s"
else
  echo "  worker jobs: disabled by SUBSTUDY_ENABLE_WORKER_JOBS=0"
fi
echo "plists:"
echo "  ${DAILY_PLIST}"
echo "  ${WEEKLY_PLIST}"
if [[ "${ENABLE_WORKER_JOBS}" != "0" ]]; then
  echo "  ${MEDIA_WORKER_PLIST}"
  echo "  ${PIPELINE_WORKER_PLIST}"
fi
echo "logs: ${LOG_DIR}"
