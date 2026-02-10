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
SOURCE_ARGS=()
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

run_substudy sync \
  --skip-ledger \
  --config "${REPO_ROOT}/config/sources.toml"

run_substudy backfill \
  --skip-ledger \
  --config "${REPO_ROOT}/config/sources.toml"

run_substudy ledger \
  --incremental \
  --config "${REPO_ROOT}/config/sources.toml"

run_substudy loudness \
  --config "${REPO_ROOT}/config/sources.toml"

run_substudy asr \
  --config "${REPO_ROOT}/config/sources.toml"
