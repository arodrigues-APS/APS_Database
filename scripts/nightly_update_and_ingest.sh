#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="/opt/aps_database/APS_Database"
COMPOSE_DIR="${REPO_ROOT}/superset"
SRC_DIR="${REPO_ROOT}/src"
PYTHON="/opt/aps_database/venv/bin/python"
BACKUP_DIR="/opt/aps_database/backups"
LOG_DIR="${REPO_ROOT}/logs/nightly"
LOCK_FILE="/tmp/aps-nightly-update-and-ingest.lock"
SUPERSET_HEALTH_URL="http://localhost:8088/health"
BACKUP_RETENTION_DAYS="${APS_BACKUP_RETENTION_DAYS:-14}"
LOG_RETENTION_DAYS="${APS_LOG_RETENTION_DAYS:-30}"
DOCKER_IMAGE_PRUNE="${APS_DOCKER_IMAGE_PRUNE:-1}"

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

validate_retention_days() {
  local name=$1
  local value=$2
  if [[ ! "${value}" =~ ^[0-9]+$ ]]; then
    die "${name} must be a non-negative integer, got: ${value}"
  fi
}

prune_docker_images() {
  if [[ "${DOCKER_IMAGE_PRUNE}" != "1" ]]; then
    log "Skipping Docker image prune; APS_DOCKER_IMAGE_PRUNE=${DOCKER_IMAGE_PRUNE}."
    return 0
  fi

  log "Pruning dangling Docker images."
  if docker image prune -f; then
    log "Docker image prune completed."
  else
    log "WARNING: Docker image prune failed; continuing after successful ingest."
  fi
}

cleanup_old_files() {
  validate_retention_days APS_BACKUP_RETENTION_DAYS "${BACKUP_RETENTION_DAYS}"
  validate_retention_days APS_LOG_RETENTION_DAYS "${LOG_RETENTION_DAYS}"

  log "Deleting database dumps older than ${BACKUP_RETENTION_DAYS} days from ${BACKUP_DIR}."
  find "${BACKUP_DIR}" -type f \( -name "mosfets-*.dump" -o -name "superset_metadata-*.dump" \) \
    -mtime +"${BACKUP_RETENTION_DAYS}" -print -delete \
    | while IFS= read -r path; do log "Deleted old backup: ${path}"; done

  log "Deleting nightly logs older than ${LOG_RETENTION_DAYS} days from ${LOG_DIR}."
  find "${LOG_DIR}" -type f -name "nightly-*.log" \
    -mtime +"${LOG_RETENTION_DAYS}" -print -delete \
    | while IFS= read -r path; do log "Deleted old nightly log: ${path}"; done
}

run_py() {
  log "Running $*"
  "${PYTHON}" "$@"
}

require_file "${REPO_ROOT}"
require_file "${COMPOSE_DIR}/docker-compose.yml"
require_file "${SRC_DIR}"
require_file "${PYTHON}"

export PATH="/opt/aps_database/venv/bin:/usr/local/bin:/usr/bin:/bin"
export PYTHONPATH="${SRC_DIR}:${PYTHONPATH:-}"
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

if [[ "$(printenv APS_UPDATE_INFRASTRUCTURE 2>/dev/null || printf '0')" == "1" ]]; then
  log "Pulling configured Docker images for an explicit maintenance window."
  docker compose pull
  log "Recreating containers with the pulled images."
  docker compose up -d --remove-orphans
else
  log "Skipping Docker image update; normal nightly ingestion does not upgrade infrastructure."
fi

wait_for_postgres "postgresqlv2" "postgres" "mosfets" "APS data database"
wait_for_postgres "superset_db" "superset" "superset" "Superset metadata database"
wait_for_superset

cd "${REPO_ROOT}"
log "Handing the data DAG to the APS Python manifest."
run_py -m aps.cli nightly run

prune_docker_images
cleanup_old_files

log "APS nightly container update and ingest completed successfully."
