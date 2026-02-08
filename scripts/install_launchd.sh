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
LABEL_PREFIX="${6:-com.bakemocho.substudy}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${REPO_ROOT}/logs"
AGENT_DIR="${HOME}/Library/LaunchAgents"
DAILY_LABEL="${LABEL_PREFIX}.daily"
WEEKLY_LABEL="${LABEL_PREFIX}.weekly_full"
DAILY_PLIST="${AGENT_DIR}/${DAILY_LABEL}.plist"
WEEKLY_PLIST="${AGENT_DIR}/${WEEKLY_LABEL}.plist"
DAILY_SCRIPT="${REPO_ROOT}/scripts/run_daily_sync.sh"
WEEKLY_SCRIPT="${REPO_ROOT}/scripts/run_weekly_full_sync.sh"
OLD_PLIST="${AGENT_DIR}/${LABEL_PREFIX}.sync.plist"

mkdir -p "${AGENT_DIR}"
mkdir -p "${LOG_DIR}"

write_plist() {
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

install_job() {
  local label="$1"
  local plist_path="$2"

  launchctl bootout "gui/${UID}/${label}" >/dev/null 2>&1 || true
  launchctl bootstrap "gui/${UID}" "${plist_path}"
  launchctl enable "gui/${UID}/${label}"
}

# Clean up older single-job setup if present
launchctl bootout "gui/${UID}/${LABEL_PREFIX}.sync" >/dev/null 2>&1 || true
if [[ -f "${OLD_PLIST}" ]]; then
  rm -f "${OLD_PLIST}"
fi

write_plist "${DAILY_PLIST}" "${DAILY_LABEL}" "${DAILY_SCRIPT}" "${DAILY_HOUR}" "${DAILY_MINUTE}"
write_plist "${WEEKLY_PLIST}" "${WEEKLY_LABEL}" "${WEEKLY_SCRIPT}" "${WEEKLY_HOUR}" "${WEEKLY_MINUTE}" "${WEEKLY_WEEKDAY}"

install_job "${DAILY_LABEL}" "${DAILY_PLIST}"
install_job "${WEEKLY_LABEL}" "${WEEKLY_PLIST}"

echo "Installed launchd jobs:"
echo "  ${DAILY_LABEL}: daily at ${DAILY_HOUR}:$(printf "%02d" "${DAILY_MINUTE}")"
echo "  ${WEEKLY_LABEL}: weekday=${WEEKLY_WEEKDAY} at ${WEEKLY_HOUR}:$(printf "%02d" "${WEEKLY_MINUTE}")"
echo "plists:"
echo "  ${DAILY_PLIST}"
echo "  ${WEEKLY_PLIST}"
echo "logs: ${LOG_DIR}"
