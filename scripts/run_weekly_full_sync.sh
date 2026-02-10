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

echo "[weekly] start: $(date '+%Y-%m-%d %H:%M:%S')"

# Keep yt-dlp fresh, but do not block weekly sync if Homebrew update fails.
if command -v brew >/dev/null 2>&1; then
  if brew list --formula yt-dlp >/dev/null 2>&1; then
    echo "[weekly] brew upgrade yt-dlp"
    if brew upgrade yt-dlp; then
      echo "[weekly] yt-dlp upgraded"
    else
      echo "[weekly] warning: yt-dlp upgrade failed; continuing" >&2
    fi
  else
    echo "[weekly] warning: yt-dlp is not a Homebrew formula; skip upgrade" >&2
  fi
else
  echo "[weekly] warning: Homebrew not found; skip yt-dlp upgrade" >&2
fi

if command -v yt-dlp >/dev/null 2>&1; then
  echo "[weekly] yt-dlp version: $(yt-dlp --version)"
fi

"${PYTHON_BIN}" "${REPO_ROOT}/scripts/substudy.py" sync --full-ledger --config "${REPO_ROOT}/config/sources.toml"
"${PYTHON_BIN}" "${REPO_ROOT}/scripts/substudy.py" loudness --config "${REPO_ROOT}/config/sources.toml"
"${PYTHON_BIN}" "${REPO_ROOT}/scripts/substudy.py" asr --config "${REPO_ROOT}/config/sources.toml"

echo "[weekly] done: $(date '+%Y-%m-%d %H:%M:%S')"
