#!/usr/bin/env bash
set -Eeuo pipefail

TARGET_REPO="${APS_DEPLOY_TARGET:-/opt/aps_database/APS_Database}"
SOURCE_REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_STATUS_PATHS=(src schema scripts superset)
TARGET_STATUS_PATHS=(src schema scripts superset)
PRESERVE_TMP=""

usage() {
  printf 'Usage: %s <committed-ref>\n' "$(basename "$0")" >&2
  printf 'Fast-forward %s to a committed ref from %s.\n' "${TARGET_REPO}" "${SOURCE_REPO}" >&2
}

log() {
  printf '[%s] %s\n' "$(date -Is)" "$*"
}

die() {
  log "ERROR: $*" >&2
  exit 1
}

source_clean() {
  git -C "${SOURCE_REPO}" status --porcelain -- "${SOURCE_STATUS_PATHS[@]}"
}

target_clean_tracked() {
  git -C "${TARGET_REPO}" status --porcelain --untracked-files=no -- "${TARGET_STATUS_PATHS[@]}"
}

is_git_checkout() {
  git -C "$1" rev-parse --git-dir >/dev/null 2>&1
}

restore_preserved_generated_artifacts() {
  local status=$?
  if [[ -n "${PRESERVE_TMP}" && -d "${PRESERVE_TMP}" ]]; then
    log "Restoring preserved generated artifacts."
    cp -a "${PRESERVE_TMP}/." "${TARGET_REPO}/"
    rm -rf "${PRESERVE_TMP}"
  fi
  return "${status}"
}
trap restore_preserved_generated_artifacts EXIT

preserve_dirty_generated_artifacts() {
  local dirty
  local line
  local relpath

  dirty="$(git -C "${TARGET_REPO}" status --porcelain --untracked-files=no -- out)"
  [[ -n "${dirty}" ]] || return 0

  PRESERVE_TMP="$(mktemp -d)"
  log "Preserving dirty generated artifacts before fast-forward:"
  while IFS= read -r line; do
    relpath="${line:3}"
    [[ -n "${relpath}" ]] || continue
    case "${relpath}" in
      out/*) ;;
      *) continue ;;
    esac
    if [[ -f "${TARGET_REPO}/${relpath}" ]]; then
      mkdir -p "${PRESERVE_TMP}/$(dirname "${relpath}")"
      cp -p "${TARGET_REPO}/${relpath}" "${PRESERVE_TMP}/${relpath}"
      git -C "${TARGET_REPO}" checkout -- "${relpath}"
      log "  ${relpath}"
    fi
  done <<< "${dirty}"
}

[[ $# -eq 1 ]] || { usage; exit 2; }
ref=$1

is_git_checkout "${SOURCE_REPO}" || die "source repo is not a git checkout: ${SOURCE_REPO}"
is_git_checkout "${TARGET_REPO}" || die "target repo is not a git checkout: ${TARGET_REPO}"

source_sha="$(git -C "${SOURCE_REPO}" rev-parse --verify "${ref}^{commit}")" \
  || die "not a committed ref in source checkout: ${ref}"
source_head="$(git -C "${SOURCE_REPO}" rev-parse --short HEAD)"
target_before="$(git -C "${TARGET_REPO}" rev-parse --short HEAD)"

log "Source repo: ${SOURCE_REPO}"
log "Target repo: ${TARGET_REPO}"
log "Requested ref: ${ref}"
log "Source HEAD: ${source_head}"
log "Deploy commit: ${source_sha}"
log "Target before: ${target_before}"

dirty_source="$(source_clean)"
if [[ -n "${dirty_source}" ]]; then
  log "Dirty source paths in source checkout; commit or stash them before deploy:"
  while IFS= read -r line; do
    [[ -n "${line}" ]] && log "  ${line}"
  done <<< "${dirty_source}"
  exit 1
fi

dirty_target="$(target_clean_tracked)"
if [[ -n "${dirty_target}" ]]; then
  log "Dirty tracked source paths in target checkout; refusing deploy:"
  while IFS= read -r line; do
    [[ -n "${line}" ]] && log "  ${line}"
  done <<< "${dirty_target}"
  exit 1
fi

log "Fetching deploy commit from source checkout."
git -C "${TARGET_REPO}" fetch --no-tags "${SOURCE_REPO}" "${source_sha}"
deploy_sha="$(git -C "${TARGET_REPO}" rev-parse --verify FETCH_HEAD^{commit})"

if [[ "${deploy_sha}" != "${source_sha}" ]]; then
  die "fetched ${deploy_sha}, expected ${source_sha}"
fi

if ! git -C "${TARGET_REPO}" merge-base --is-ancestor HEAD "${deploy_sha}"; then
  die "target HEAD is not an ancestor of ${deploy_sha}; deploy would not be a fast-forward"
fi

preserve_dirty_generated_artifacts

log "Fast-forwarding target checkout."
git -C "${TARGET_REPO}" merge --ff-only "${deploy_sha}"

target_after="$(git -C "${TARGET_REPO}" rev-parse --short HEAD)"
log "Target after: ${target_after}"
log "Target tracked source status after deploy:"
after_status="$(target_clean_tracked)"
if [[ -n "${after_status}" ]]; then
  while IFS= read -r line; do
    [[ -n "${line}" ]] && log "  ${line}"
  done <<< "${after_status}"
else
  log "  clean"
fi
