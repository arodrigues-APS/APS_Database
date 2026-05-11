#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="/opt/aps_database/APS_Database"
COMPOSE_DIR="${REPO_ROOT}/superset"
INGEST_DIR="${REPO_ROOT}/data_processing_scripts"
PYTHON="/opt/aps_database/venv/bin/python"
BACKUP_DIR="/opt/aps_database/backups"
LOG_DIR="${REPO_ROOT}/logs/nightly"
LOCK_FILE="/tmp/aps-nightly-update-and-ingest.lock"
SUPERSET_HEALTH_URL="http://localhost:8088/health"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "${BACKUP_DIR}" "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/nightly-${timestamp}.log"
exec > >(tee -a "${LOG_FILE}") 2>&1

log() {
  printf '[%s] %s\n' "$(date -Is)" "$*"
}

die() {
  log "ERROR: $*"
  exit 1
}

on_error() {
  local status=$?
  log "FAILED at line ${BASH_LINENO[0]} with exit status ${status}"
  exit "${status}"
}
trap on_error ERR

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  log "Another APS nightly update/ingest run is already active; exiting."
  exit 0
fi

require_file() {
  local path=$1
  [[ -e "${path}" ]] || die "Missing required path: ${path}"
}

check_python_modules() {
  local missing
  missing="$("${PYTHON}" - <<'PY'
modules = [
    "h5py",
    "joblib",
    "luaparser",
    "matplotlib",
    "numpy",
    "openpyxl",
    "pandas",
    "psycopg2",
    "requests",
    "scipy",
    "sklearn",
]
missing = []
for module in modules:
    try:
        __import__(module)
    except Exception as exc:
        missing.append(f"{module}: {exc}")
print("\n".join(missing))
PY
)"
  if [[ -n "${missing}" ]]; then
    die "Python environment ${PYTHON} is missing required modules: ${missing}"
  fi
}

wait_for_postgres() {
  local container=$1
  local user=$2
  local database=$3
  local label=$4

  log "Waiting for ${label} (${container}/${database})..."
  for _ in {1..60}; do
    if docker exec "${container}" pg_isready -U "${user}" -d "${database}" >/dev/null 2>&1; then
      log "${label} is ready."
      return 0
    fi
    sleep 5
  done
  die "${label} did not become ready in time."
}

wait_for_superset() {
  log "Waiting for Superset health endpoint..."
  for _ in {1..60}; do
    if curl -fsS "${SUPERSET_HEALTH_URL}" >/dev/null; then
      log "Superset health endpoint is ready."
      return 0
    fi
    sleep 5
  done
  die "Superset health endpoint did not become ready in time."
}

dump_database() {
  local container=$1
  local user=$2
  local database=$3
  local output=$4
  local tmp="${output}.partial"

  log "Backing up ${container}/${database} to ${output}"
  rm -f "${tmp}"
  docker exec "${container}" pg_dump -U "${user}" -d "${database}" -Fc > "${tmp}"
  [[ -s "${tmp}" ]] || die "Backup is empty: ${tmp}"
  mv "${tmp}" "${output}"
}

run_py() {
  log "Running $*"
  "${PYTHON}" "$@"
}

require_file "${REPO_ROOT}"
require_file "${COMPOSE_DIR}/docker-compose.yml"
require_file "${INGEST_DIR}"
require_file "${PYTHON}"

export PATH="/opt/aps_database/venv/bin:/usr/local/bin:/usr/bin:/bin"
export PYTHONPATH="${INGEST_DIR}:${REPO_ROOT}:${PYTHONPATH:-}"
export PYTHONUNBUFFERED=1

log "Starting APS nightly container update and ingest."
log "Repository root: ${REPO_ROOT}"
log "Log file: ${LOG_FILE}"
check_python_modules

cd "${COMPOSE_DIR}"
wait_for_postgres "postgresqlv2" "postgres" "mosfets" "APS data database"
wait_for_postgres "superset_db" "superset" "superset" "Superset metadata database"

dump_database "postgresqlv2" "postgres" "mosfets" \
  "${BACKUP_DIR}/mosfets-${timestamp}.dump"
dump_database "superset_db" "superset" "superset" \
  "${BACKUP_DIR}/superset_metadata-${timestamp}.dump"

log "Pulling configured Docker images."
docker compose pull

log "Recreating containers with the pulled images."
docker compose up -d --remove-orphans

wait_for_postgres "postgresqlv2" "postgres" "mosfets" "APS data database"
wait_for_postgres "superset_db" "superset" "superset" "Superset metadata database"
wait_for_superset

cd "${INGEST_DIR}"
run_py seed_device_library.py
run_py seed_device_mapping_rules.py
run_py ingestion_baselines.py
run_py ingestion_sc.py
run_py seed_irradiation_campaigns.py
run_py ingestion_irradiation.py
run_py parse_logbooks_assign_runs.py
run_py ingestion_avalanche.py
run_py -c "from db_config import get_connection; conn=get_connection(); cur=conn.cursor(); cur.execute('REFRESH MATERIALIZED VIEW baselines_run_max_current'); conn.commit(); cur.close(); conn.close(); print('refreshed baselines_run_max_current')"
run_py extract_damage_metrics.py
run_py create_baselines_dashboard.py
if [[ -f create_baselines_dashboard_device_library.py ]]; then
  run_py create_baselines_dashboard_device_library.py
else
  log "Skipping create_baselines_dashboard_device_library.py; not present in this checkout."
fi
run_py create_sc_dashboard.py
run_py create_irradiation_dashboard.py
run_py create_avalanche_dashboard.py
run_py ml_sc_irrad_equivalence.py --rebuild
run_py create_sc_irrad_dashboard.py
run_py create_iv_physical_prediction_dashboard.py

log "APS nightly container update and ingest completed successfully."
