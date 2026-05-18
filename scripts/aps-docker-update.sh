#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="/opt/aps_database/APS_Database"
COMPOSE_DIR="${REPO_ROOT}/superset"
LOG_DIR="${REPO_ROOT}/logs/docker-update"
LOCK_FILE="/tmp/aps-docker-update.lock"
SUPERSET_HEALTH_URL="http://localhost:8088/health"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/docker-update-${timestamp}.log"
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
  log "Another APS Docker update is already active; exiting."
  exit 0
fi

require_path() {
  local path=$1
  [[ -e "${path}" ]] || die "Missing required path: ${path}"
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

wait_for_redis() {
  log "Waiting for Redis..."
  for _ in {1..60}; do
    if docker exec superset_cache redis-cli ping >/dev/null 2>&1; then
      log "Redis is ready."
      return 0
    fi
    sleep 5
  done
  die "Redis did not become ready in time."
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

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

require_path "${COMPOSE_DIR}/docker-compose.yml"

log "Starting APS Docker Compose update."
log "Compose directory: ${COMPOSE_DIR}"
log "Log file: ${LOG_FILE}"

cd "${COMPOSE_DIR}"

log "Current Compose status:"
docker compose ps

log "Pulling configured Docker images."
docker compose pull

log "Recreating containers with pulled images."
docker compose up -d --remove-orphans

wait_for_postgres "postgresqlv2" "postgres" "mosfets" "APS data database"
wait_for_postgres "superset_db" "superset" "superset" "Superset metadata database"
wait_for_redis
wait_for_superset

log "Updated Compose status:"
docker compose ps

log "APS Docker Compose update completed successfully."
