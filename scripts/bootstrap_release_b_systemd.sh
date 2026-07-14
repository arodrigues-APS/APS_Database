#!/usr/bin/env bash
set -Eeuo pipefail

# Install the Release B systemd/environment boundary without deploying code,
# restarting the web application, or enabling the nightly timer.

REPO_ROOT="${APS_RELEASE_SOURCE:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
ENV_DIR="/etc/aps"
ENV_FILE="${ENV_DIR}/aps.env"
ENV_TEMPLATE="${REPO_ROOT}/server_config/aps.env.example"
BACKUP_DIR="${ENV_DIR}/systemd-pre-release-b"
SERVICE_USER="arodrigues"
SERVICE_GROUP="www-data"
UNITS=(
  "server.service"
  "aps-nightly.service"
  "aps-nightly.timer"
  "aps-nightly-failure-record@.service"
)

log() {
  printf "[release-b-bootstrap] %s\n" "$*"
}

die() {
  log "ERROR: $*" >&2
  exit 1
}

read_env_value() {
  local key=$1
  awk -v wanted="${key}" '
    /^[[:space:]]*(#|$)/ { next }
    {
      line = $0
      sub(/^[[:space:]]*/, "", line)
      separator = index(line, "=")
      if (separator > 0 && substr(line, 1, separator - 1) == wanted) {
        count += 1
        value = substr(line, separator + 1)
      }
    }
    END {
      if (count != 1) {
        exit 1
      }
      print value
    }
  ' "${ENV_FILE}"
}

normalize_value() {
  local value=$1
  value="${value#\"}"
  value="${value%\"}"
  value="${value#\'}"
  value="${value%\'}"
  printf "%s" "${value}"
}

require_configured_key() {
  local key=$1
  local value
  local normalized
  if ! value="$(read_env_value "${key}")"; then
    die "${ENV_FILE} must contain exactly one ${key}= assignment"
  fi
  normalized="$(normalize_value "${value}")"
  [[ -n "${normalized}" ]] || die "${key} must not be empty"
  case "${normalized,,}" in
    change-me|replace-me|changeme|password|secret)
      die "${key} still contains a placeholder value"
      ;;
  esac
  printf "%s" "${normalized}"
}

require_service_directory() {
  local key=$1
  local mode=$2
  local path
  path="$(require_configured_key "${key}")"
  [[ "${path}" == /* ]] || die "${key} must be an absolute path"
  runuser -u "${SERVICE_USER}" -- test -d "${path}" || die "${key} is not a directory accessible to ${SERVICE_USER}: ${path}"
  runuser -u "${SERVICE_USER}" -- test -r "${path}" || die "${key} is not readable by ${SERVICE_USER}: ${path}"
  runuser -u "${SERVICE_USER}" -- test -x "${path}" || die "${key} is not traversable by ${SERVICE_USER}: ${path}"
  if [[ "${mode}" == "write" ]]; then
    runuser -u "${SERVICE_USER}" -- test -w "${path}" || die "${key} is not writable by ${SERVICE_USER}: ${path}"
  fi
}

[[ "${EUID}" -eq 0 ]] || die "run this script through sudo"
[[ -f "${ENV_TEMPLATE}" ]] || die "missing environment template: ${ENV_TEMPLATE}"
id "${SERVICE_USER}" >/dev/null 2>&1 || die "unknown service user: ${SERVICE_USER}"
getent group "${SERVICE_GROUP}" >/dev/null || die "unknown service group: ${SERVICE_GROUP}"
getent group docker >/dev/null || die "required docker group is absent"

for unit in "${UNITS[@]}"; do
  [[ -f "${REPO_ROOT}/server_config/${unit}" ]] || die "missing tracked unit: server_config/${unit}"
done

install -d -o root -g root -m 0755 "${ENV_DIR}"
if [[ ! -e "${ENV_FILE}" ]]; then
  install -o root -g root -m 0600 "${ENV_TEMPLATE}" "${ENV_FILE}"
  log "Created ${ENV_FILE} from the tracked template."
  log "Edit it with sudoedit, replace every placeholder, set this host's data paths,"
  log "then rerun this script. No units were installed."
  exit 3
fi

[[ -f "${ENV_FILE}" && ! -L "${ENV_FILE}" ]] || die "${ENV_FILE} must be a regular file, not a symlink"
chown root:root "${ENV_FILE}"
chmod 0600 "${ENV_FILE}"

required_keys=(
  APS_PROFILE
  APS_DB_HOST
  APS_DB_PORT
  APS_DB_NAME
  APS_DB_USER
  APS_DB_PASSWORD
  APS_SUPERSET_URL
  APS_SUPERSET_USER
  APS_SUPERSET_PASS
  APS_FLASK_SECRET_KEY
  APS_DATA_ROOT
  APS_NAS_ROOT
  APS_ENABLE_LEGACY_CV_DPT
  APS_WEB_TOOLS_DIR
)
for key in "${required_keys[@]}"; do
  require_configured_key "${key}" >/dev/null
done

profile="$(require_configured_key APS_PROFILE)"
[[ "${profile}" == "production" ]] || die "APS_PROFILE must be production for this host"
legacy_cv="$(require_configured_key APS_ENABLE_LEGACY_CV_DPT)"
[[ "${legacy_cv}" == "0" ]] || die "APS_ENABLE_LEGACY_CV_DPT must remain 0 for Release B"

require_service_directory APS_DATA_ROOT read
require_service_directory APS_NAS_ROOT read
require_service_directory APS_WEB_TOOLS_DIR write

systemd-analyze verify "${REPO_ROOT}/server_config/server.service" "${REPO_ROOT}/server_config/aps-nightly.service" "${REPO_ROOT}/server_config/aps-nightly.timer" "${REPO_ROOT}/server_config/aps-nightly-failure-record@.service"

install -d -o root -g root -m 0700 "${BACKUP_DIR}"
for unit in "${UNITS[@]}"; do
  source_path="${REPO_ROOT}/server_config/${unit}"
  target_path="/etc/systemd/system/${unit}"
  backup_path="${BACKUP_DIR}/${unit}"
  if [[ -e "${target_path}" && ! -e "${backup_path}" ]]; then
    cp --preserve=mode,ownership,timestamps "${target_path}" "${backup_path}"
    log "Preserved pre-Release B unit: ${backup_path}"
  fi
  install -o root -g root -m 0644 "${source_path}" "${target_path}"
  log "Installed ${target_path}"
done

systemctl daemon-reload
systemctl disable --now aps-nightly.timer

systemctl is-active --quiet aps-nightly.timer && die "aps-nightly.timer unexpectedly remained active"
if systemctl is-enabled --quiet aps-nightly.timer; then
  die "aps-nightly.timer unexpectedly remained enabled"
fi

log "Bootstrap complete."
log "The nightly timer is disabled and inactive."
log "The web service was not restarted and the nightly service was not started."
log "Continue only with the documented deploy, migration, model, smoke, and shadow gates."
