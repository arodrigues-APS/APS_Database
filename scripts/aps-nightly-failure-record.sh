#!/usr/bin/env bash
set -Eeuo pipefail

UNIT="${1:-aps-nightly.service}"
REPO_ROOT="/opt/aps_database/APS_Database"
LOG_DIR="${REPO_ROOT}/logs/nightly"
FAILURE_LOG="${LOG_DIR}/failures.log"
LOCK_FILE="/tmp/aps-nightly-failure-record.lock"

mkdir -p "${LOG_DIR}"

latest_log() {
  find "${LOG_DIR}" -maxdepth 1 -type f -name "nightly-*.log" \
    -printf "%T@ %p\n" 2>/dev/null \
    | sort -nr \
    | awk 'NR == 1 {print $2}'
}

{
  flock -n 9 || exit 0
  log_file="$(latest_log || true)"
  {
    printf '[%s] APS nightly failure detected\n' "$(date -Is)"
    printf '  unit=%s\n' "${UNIT}"
    printf '  host=%s\n' "$(hostname -f 2>/dev/null || hostname)"
    if [[ -n "${log_file}" ]]; then
      printf '  latest_log=%s\n' "${log_file}"
    else
      printf '  latest_log=<none>\n'
    fi
    printf '  inspect=systemctl --user status %s --no-pager\n' "${UNIT}"
    printf '  journal=journalctl --user -u %s -n 120 --no-pager\n' "${UNIT}"
  } >> "${FAILURE_LOG}"
} 9>"${LOCK_FILE}"
