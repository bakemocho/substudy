#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

tmp_output="$(mktemp)"
cleanup() {
  rm -f "${tmp_output}"
}
trap cleanup EXIT

# Detect user-specific absolute home paths that should not be committed.
if git ls-files -z \
  | xargs -0 rg -n --no-heading --pcre2 \
    -e '/Users/[A-Za-z0-9._-]+/' \
    -e '/home/[A-Za-z0-9._-]+/' \
    -e 'C:\\Users\\[A-Za-z0-9._-]+\\' \
    > "${tmp_output}"; then
  echo "privacy check failed: tracked files contain user-specific absolute paths." >&2
  cat "${tmp_output}" >&2
  exit 1
fi

echo "privacy check passed"
