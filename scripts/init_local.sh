#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONFIG_DIR="${REPO_ROOT}/config"

SOURCES_EXAMPLE="${CONFIG_DIR}/sources.example.toml"
SOURCES_LOCAL="${CONFIG_DIR}/sources.toml"
TARGETS_LOCAL="${CONFIG_DIR}/source_targets.json"

echo "[init-local] repo=${REPO_ROOT}"

if [[ ! -f "${SOURCES_EXAMPLE}" ]]; then
  echo "[init-local] error: missing template: ${SOURCES_EXAMPLE}" >&2
  exit 1
fi

if [[ -f "${SOURCES_LOCAL}" ]]; then
  echo "[init-local] keep existing ${SOURCES_LOCAL}"
else
  cp "${SOURCES_EXAMPLE}" "${SOURCES_LOCAL}"
  echo "[init-local] created ${SOURCES_LOCAL} from template"
fi

if [[ -f "${TARGETS_LOCAL}" ]]; then
  echo "[init-local] keep existing ${TARGETS_LOCAL}"
else
  cat >"${TARGETS_LOCAL}" <<'JSON'
{
  "targets": []
}
JSON
  echo "[init-local] created ${TARGETS_LOCAL}"
fi

echo "[init-local] done"
