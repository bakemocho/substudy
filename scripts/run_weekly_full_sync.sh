#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

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

python3 "${REPO_ROOT}/scripts/substudy.py" sync --full-ledger --config "${REPO_ROOT}/config/sources.toml"
python3 "${REPO_ROOT}/scripts/substudy.py" loudness --config "${REPO_ROOT}/config/sources.toml"
python3 "${REPO_ROOT}/scripts/substudy.py" asr --config "${REPO_ROOT}/config/sources.toml"

echo "[weekly] done: $(date '+%Y-%m-%d %H:%M:%S')"
