#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"
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

python3 "${REPO_ROOT}/scripts/substudy.py" sync \
  --skip-ledger \
  --config "${REPO_ROOT}/config/sources.toml" \
  "${SOURCE_ARGS[@]}"

python3 "${REPO_ROOT}/scripts/substudy.py" backfill \
  --skip-ledger \
  --config "${REPO_ROOT}/config/sources.toml" \
  "${SOURCE_ARGS[@]}"

python3 "${REPO_ROOT}/scripts/substudy.py" ledger \
  --incremental \
  --config "${REPO_ROOT}/config/sources.toml" \
  "${SOURCE_ARGS[@]}"

python3 "${REPO_ROOT}/scripts/substudy.py" asr \
  --config "${REPO_ROOT}/config/sources.toml" \
  "${SOURCE_ARGS[@]}"
