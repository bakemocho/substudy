#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

DEFAULT_CONFIG_PATH="${REPO_ROOT}/config/sources.toml"
DEFAULT_LEDGER_DB_PATH="${REPO_ROOT}/data/master_ledger.sqlite"
DEFAULT_EXPORT_DIR="${REPO_ROOT}/exports/llm"
DEFAULT_MISSING_REVIEW_PATH="${DEFAULT_EXPORT_DIR}/missing_review.jsonl"
DEFAULT_ENRICHED_MISSING_PATH="${DEFAULT_EXPORT_DIR}/enriched_missing.jsonl"
DEFAULT_REVIEW_CARDS_PATH="${DEFAULT_EXPORT_DIR}/review_cards.jsonl"

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

  echo "[llm-pipeline] error: Python 3.11+ interpreter not found." >&2
  echo "[llm-pipeline] set SUBSTUDY_PYTHON to a compatible executable." >&2
  return 1
}

usage() {
  cat <<'EOF'
Usage:
  scripts/run_llm_pipeline.sh <command> [options]

Commands:
  preflight            Validate required files and dirs.
  missing-export       Export missing-review JSONL.
  missing-import       Dry-run + safe import for enriched missing JSONL.
  review-cards-export  Export review-cards JSONL.

Options:
  --config <path>         Config TOML path.
  --ledger-db <path>      Ledger DB path.
  --limit <int>           Curate output limit (default: 200).
  --source <id>           Optional source filter (repeatable).
  --missing-output <path> missing_review output path.
  --enriched-input <path> enriched_missing input path.
  --review-output <path>  review_cards output path.
EOF
}

if (($# == 0)); then
  usage >&2
  exit 1
fi

COMMAND="$1"
shift

CONFIG_PATH="${DEFAULT_CONFIG_PATH}"
LEDGER_DB_PATH="${DEFAULT_LEDGER_DB_PATH}"
LIMIT="200"
MISSING_OUTPUT_PATH="${DEFAULT_MISSING_REVIEW_PATH}"
ENRICHED_INPUT_PATH="${DEFAULT_ENRICHED_MISSING_PATH}"
REVIEW_OUTPUT_PATH="${DEFAULT_REVIEW_CARDS_PATH}"
SOURCE_ARGS=()

while (($# > 0)); do
  case "$1" in
    --config)
      [[ $# -ge 2 ]] || { echo "error: --config requires a value" >&2; exit 1; }
      CONFIG_PATH="$2"
      shift 2
      ;;
    --ledger-db)
      [[ $# -ge 2 ]] || { echo "error: --ledger-db requires a value" >&2; exit 1; }
      LEDGER_DB_PATH="$2"
      shift 2
      ;;
    --limit)
      [[ $# -ge 2 ]] || { echo "error: --limit requires a value" >&2; exit 1; }
      LIMIT="$2"
      shift 2
      ;;
    --missing-output)
      [[ $# -ge 2 ]] || { echo "error: --missing-output requires a value" >&2; exit 1; }
      MISSING_OUTPUT_PATH="$2"
      shift 2
      ;;
    --enriched-input)
      [[ $# -ge 2 ]] || { echo "error: --enriched-input requires a value" >&2; exit 1; }
      ENRICHED_INPUT_PATH="$2"
      shift 2
      ;;
    --review-output)
      [[ $# -ge 2 ]] || { echo "error: --review-output requires a value" >&2; exit 1; }
      REVIEW_OUTPUT_PATH="$2"
      shift 2
      ;;
    --source)
      [[ $# -ge 2 ]] || { echo "error: --source requires a value" >&2; exit 1; }
      SOURCE_ARGS+=("$1" "$2")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unsupported argument '$1'" >&2
      usage >&2
      exit 1
      ;;
  esac
done

PYTHON_BIN="$(resolve_python_bin)"

run_substudy() {
  local subcommand="$1"
  shift
  local -a cmd=(
    "${PYTHON_BIN}"
    "${REPO_ROOT}/scripts/substudy.py"
    "${subcommand}"
    --config "${CONFIG_PATH}"
    --ledger-db "${LEDGER_DB_PATH}"
  )
  if ((${#SOURCE_ARGS[@]} > 0)); then
    cmd+=("${SOURCE_ARGS[@]}")
  fi
  cmd+=("$@")
  "${cmd[@]}"
}

preflight() {
  if [[ ! -f "${REPO_ROOT}/scripts/substudy.py" ]]; then
    echo "[llm-pipeline] error: substudy.py not found: ${REPO_ROOT}/scripts/substudy.py" >&2
    return 1
  fi
  if [[ ! -f "${CONFIG_PATH}" ]]; then
    echo "[llm-pipeline] error: config not found: ${CONFIG_PATH}" >&2
    return 1
  fi
  if [[ ! -f "${LEDGER_DB_PATH}" ]]; then
    echo "[llm-pipeline] error: ledger DB not found: ${LEDGER_DB_PATH}" >&2
    return 1
  fi
  mkdir -p "${DEFAULT_EXPORT_DIR}"
}

jsonl_line_count() {
  local path="$1"
  if [[ ! -f "${path}" ]]; then
    echo "0"
    return 0
  fi
  wc -l < "${path}" | tr -d '[:space:]'
}

run_missing_export() {
  preflight
  mkdir -p "$(dirname -- "${MISSING_OUTPUT_PATH}")"
  run_substudy dict-bookmarks-curate \
    --preset missing_review \
    --format jsonl \
    --limit "${LIMIT}" \
    --output "${MISSING_OUTPUT_PATH}"
  if [[ ! -f "${MISSING_OUTPUT_PATH}" ]]; then
    : > "${MISSING_OUTPUT_PATH}"
  fi
  local rows
  rows="$(jsonl_line_count "${MISSING_OUTPUT_PATH}")"
  echo "[llm-pipeline] missing-export rows=${rows} output=${MISSING_OUTPUT_PATH}"
}

run_missing_import() {
  preflight
  mkdir -p "$(dirname -- "${ENRICHED_INPUT_PATH}")"
  if [[ ! -f "${ENRICHED_INPUT_PATH}" ]]; then
    : > "${ENRICHED_INPUT_PATH}"
    echo "[llm-pipeline] missing-import created placeholder: ${ENRICHED_INPUT_PATH}"
  fi

  local dry_output=""
  if ! dry_output="$(run_substudy dict-bookmarks-import \
    --input "${ENRICHED_INPUT_PATH}" \
    --on-duplicate upsert \
    --dry-run 2>&1)"; then
    printf '%s\n' "${dry_output}"
    echo "[llm-pipeline] missing-import dry-run failed" >&2
    return 1
  fi
  printf '%s\n' "${dry_output}"

  if printf '%s\n' "${dry_output}" | grep -q "\[dict-bookmarks-import\] no rows in "; then
    echo "[llm-pipeline] missing-import no-op: no rows in ${ENRICHED_INPUT_PATH}"
    return 0
  fi

  local dry_errors=""
  dry_errors="$(
    printf '%s\n' "${dry_output}" \
      | sed -n 's/.*errors=\([0-9][0-9]*\).*/\1/p' \
      | tail -n 1
  )"

  if [[ -z "${dry_errors}" ]]; then
    echo "[llm-pipeline] missing-import could not parse dry-run errors count; skip actual import" >&2
    return 1
  fi
  if [[ "${dry_errors}" != "0" ]]; then
    echo "[llm-pipeline] missing-import blocked: dry-run errors=${dry_errors}; skip actual import"
    return 0
  fi

  run_substudy dict-bookmarks-import \
    --input "${ENRICHED_INPUT_PATH}" \
    --on-duplicate upsert
}

run_review_cards_export() {
  preflight
  mkdir -p "$(dirname -- "${REVIEW_OUTPUT_PATH}")"
  run_substudy dict-bookmarks-curate \
    --preset review_cards \
    --format jsonl \
    --limit "${LIMIT}" \
    --output "${REVIEW_OUTPUT_PATH}"
  if [[ ! -f "${REVIEW_OUTPUT_PATH}" ]]; then
    : > "${REVIEW_OUTPUT_PATH}"
  fi
  local rows
  rows="$(jsonl_line_count "${REVIEW_OUTPUT_PATH}")"
  echo "[llm-pipeline] review-cards-export rows=${rows} output=${REVIEW_OUTPUT_PATH}"
}

case "${COMMAND}" in
  preflight)
    preflight
    echo "[llm-pipeline] preflight ok config=${CONFIG_PATH} ledger_db=${LEDGER_DB_PATH}"
    ;;
  missing-export)
    run_missing_export
    ;;
  missing-import)
    run_missing_import
    ;;
  review-cards-export)
    run_review_cards_export
    ;;
  *)
    echo "error: unsupported command '${COMMAND}'" >&2
    usage >&2
    exit 1
    ;;
esac
