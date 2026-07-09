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
WEB_TOOLS_DIR="${APS_WEB_TOOLS_DIR:-/data/www/tools}"
DAMAGE_SIGNATURE_VIEWER_HTML="${REPO_ROOT}/out/avalanche_irrad_pilot/damage_signature_3d_interactive.html"
SOURCE_STATUS_PATHS=(src schema scripts superset)

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

run_py_optional() {
  log "Running optional $*"
  if ! "${PYTHON}" "$@"; then
    log "WARNING: optional Python step failed: $*"
    return 1
  fi
}

publish_damage_signature_viewer_optional() {
  local src="${DAMAGE_SIGNATURE_VIEWER_HTML}"
  local dest_dir="${WEB_TOOLS_DIR}/damage-signature-3d"
  local legacy_dir="${WEB_TOOLS_DIR}/phenotype-3d"

  if [[ ! -s "${src}" ]]; then
    log "WARNING: damage-signature viewer artifact missing or empty: ${src}"
    return 1
  fi

  log "Publishing damage-signature viewer to ${dest_dir}/index.html"
  mkdir -p "${dest_dir}"
  cp "${src}" "${dest_dir}/index.html.tmp"
  chmod 0644 "${dest_dir}/index.html.tmp"
  mv "${dest_dir}/index.html.tmp" "${dest_dir}/index.html"

  if [[ -d "${legacy_dir}" || -w "${WEB_TOOLS_DIR}" ]]; then
    mkdir -p "${legacy_dir}"
    cat > "${legacy_dir}/index.html.tmp" <<'HTML'
<!doctype html>
<meta charset="utf-8">
<meta http-equiv="refresh" content="0; url=/tools/damage-signature-3d/">
<title>Redirecting to damage-signature viewer</title>
<link rel="canonical" href="/tools/damage-signature-3d/">
<p>This viewer moved to <a href="/tools/damage-signature-3d/">/tools/damage-signature-3d/</a>.</p>
HTML
    chmod 0644 "${legacy_dir}/index.html.tmp"
    mv "${legacy_dir}/index.html.tmp" "${legacy_dir}/index.html"
  else
    log "WARNING: cannot update legacy phenotype viewer directory: ${legacy_dir}"
  fi
}

preflight_irradiation_seed_source() {
  local head
  local dirty

  if ! head="$(git -C "${REPO_ROOT}" rev-parse --short HEAD 2>/dev/null)"; then
    log "WARNING: unable to read git HEAD for ${REPO_ROOT}; skipping irradiation seed."
    return 1
  fi
  log "Repository HEAD before irradiation seed: ${head}"

  if ! dirty="$(git -C "${REPO_ROOT}" status --short --untracked-files=no -- "${SOURCE_STATUS_PATHS[@]}" 2>&1)"; then
    log "WARNING: unable to inspect source cleanliness; skipping irradiation seed."
    while IFS= read -r line; do
      [[ -n "${line}" ]] && log "  ${line}"
    done <<< "${dirty}"
    return 1
  fi

  if [[ -n "${dirty}" ]]; then
    log "WARNING: dirty tracked source files detected; skipping irradiation seed only."
    while IFS= read -r line; do
      [[ -n "${line}" ]] && log "  ${line}"
    done <<< "${dirty}"
    return 1
  fi

  log "Tracked source paths clean for irradiation seed."
  return 0
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

log "Pulling configured Docker images."
docker compose pull

log "Recreating containers with the pulled images."
docker compose up -d --remove-orphans

wait_for_postgres "postgresqlv2" "postgres" "mosfets" "APS data database"
wait_for_postgres "superset_db" "superset" "superset" "Superset metadata database"
wait_for_superset

cd "${REPO_ROOT}"
run_py -m aps.seeds.seed_device_library
run_py -m aps.seeds.seed_device_mapping_rules
run_py -m aps.ingest.ingestion_baselines
run_py -m aps.ingest.ingestion_sc
if preflight_irradiation_seed_source; then
  run_py -m aps.seeds.seed_irradiation_campaigns
else
  log "WARNING: continuing downstream without seed_irradiation_campaigns."
fi
run_py -m aps.ingest.ingestion_irradiation
run_py -m aps.ingest.parse_logbooks_assign_runs
run_py -m aps.enrich.irradiation_energy_windows
run_py -m aps.enrich.extract_single_event_effects
run_py -m aps.enrich.radiation_stress_dose
run_py -m aps.ingest.ingestion_avalanche
run_py -c "from aps.db_config import get_connection; conn=get_connection(); cur=conn.cursor(); cur.execute('REFRESH MATERIALIZED VIEW baselines_run_max_current'); conn.commit(); cur.close(); conn.close(); print('refreshed baselines_run_max_current')"
run_py -m aps.enrich.extract_damage_metrics
run_py -m aps.superset.create_baselines_dashboard
if [[ -f "${SRC_DIR}/aps/superset/create_baselines_dashboard_device_library.py" ]]; then
  run_py -m aps.superset.create_baselines_dashboard_device_library
else
  log "Skipping create_baselines_dashboard_device_library; not present in this checkout."
fi
run_py -m aps.superset.create_sc_dashboard
run_py -m aps.superset.create_irradiation_dashboard
run_py -m aps.superset.create_avalanche_dashboard
run_py -m aps.ml.ml_post_iv_physical_prediction \
  --rebuild-sql \
  --extract-features \
  --build-pairs \
  --include-library-pristine \
  --train \
  --validate \
  --validation-mode both \
  --reference-tier both \
  --predict-curves
run_py -m aps.superset.create_iv_physical_prediction_dashboard
run_py -m aps.ml.ml_sc_irrad_equivalence --rebuild
run_py -m aps.superset.create_proxy_readiness_dashboard
# The self-contained interactive viewer is an exported artifact, not a core
# ingest dependency. Keep it fresh when possible, but do not abort nightly
# ingestion if a viewer-only export/regeneration step fails.
run_py_optional -m aps.proxy.apply_mechanistic_energy_proxy || true
run_py_optional -m aps.viewers.plot_source_damage_signature_3d || true
run_py_optional -m aps.viewers.plot_damage_signature_delta_3d || true
run_py_optional -m aps.exports.export_proxy_candidate_energy_v2_csv || true
run_py_optional -m aps.exports.export_proxy_method_concordance_csv || true
run_py_optional -m aps.exports.export_proxy_candidate_combined_v3_csv || true
run_py_optional -m aps.viewers.create_interactive_damage_signature_viewer || true
publish_damage_signature_viewer_optional || true
run_py -m aps.superset.create_sc_irrad_dashboard
run_py -m aps.superset.create_sc_irrad_prediction_dashboard

prune_docker_images
cleanup_old_files

log "APS nightly container update and ingest completed successfully."
