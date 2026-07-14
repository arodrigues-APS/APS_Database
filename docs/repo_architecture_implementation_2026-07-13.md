# Repository architecture implementation — 2026-07-13

## Purpose and authority

This is the implementation record for
`docs/repo_architecture_review_2026-07-06.md`. It records repository edits,
decisions, verification evidence, release operations, rollback evidence, and
accepted deferrals for Releases A–C.

The release strategy is intentionally staged:

1. **Release A — foundation and boot safety:** configuration, dependency and
   package boundaries, truthful source provenance, forward-migration adoption,
   removal of request-time DDL, service hardening, CI, and documentation.
2. **Release B — database and nightly lifecycle:** explicit adoption of the
   existing database, new forward migrations, repeatable model ownership,
   attended nightly shadow execution, and scheduler enablement only after
   verification.
3. **Release C — scientific and presentation changes:** proxy SQL, dashboard,
   export, viewer, and portfolio changes reviewed independently from the
   runtime foundation.

No release is successful merely because repository tests pass. A release is
successful only when its documented acceptance criteria and recovery gates
are satisfied.

## Adopted decisions

- Preserve the one-server monolith and raw psycopg2 architecture.
- Keep forward structural migrations separate from repeatable analytical
  models.
- Use staged releases instead of deploying the mixed worktree as one unit.
- Force-track this implementation record and the architecture review while
  keeping other personal `docs/` material ignored. The CIFS filesystem marks
  `.gitignore` immutable and does not support clearing that flag, so a durable
  ignore exception cannot be added there.
- Keep legacy CV/DPT committed but disabled with
  `APS_ENABLE_LEGACY_CV_DPT=0` until a canonical importer and parity record
  exist.
- Run the first nightly service as `arodrigues`, not root, subject to verified
  Docker, backup, log, artifact, and mount permissions.
- Add an explicit historical migration baseline boundary rather than
  baselining every discovered SQL asset.
- Refuse dirty production model/nightly executions and record a source-tree
  fingerprint.
- Separate scientific proxy/dashboard/viewer changes from the foundation
  release.

## Preservation and recovery record

### Initial source state

- Original branch/SHA: `master` at `8baa144` (matching `origin/master` at the
  start of implementation).
- Preservation branch ref created:
  `architecture-foundation-2026-07` at `8baa144`.
- Initial working tree: 39 tracked files modified and 32 untracked files,
  plus the previously ignored architecture review.
- No production database, Superset, Docker, systemd, credential, backup, or
  deployment mutation was performed while establishing this record.

### CIFS Git metadata incident

The source checkout is on a CIFS mount. During preservation-branch creation,
Git created the new branch ref but the atomic `HEAD` update failed with
`Permission denied`. The host-side `.git` directory subsequently became
intermittently visible but unusable because `HEAD` was absent and the CIFS
directory could not be renamed.

The original metadata visible through the execution environment was copied to
`/tmp/aps_database_git_recovery_20260713`, and a new `HEAD` was added there
pointing to `refs/heads/architecture-foundation-2026-07`.

Recovery verification:

- branch: `architecture-foundation-2026-07`;
- SHA: `8baa144`;
- `git fsck --full --no-reflogs`: successful; only normal dangling historical
  objects were reported;
- recovered index: reproduced the complete dirty working-tree inventory; and
- recovery archive:
  `out/git-metadata-recovery-20260713.tar.gz`, SHA-256
  `9f643016ea0a308d2ee637a03a27a184b490fae338b242d72966ba8c2d9632a8`.

Direct SMB inspection later established the precise failure mode:
`.git/HEAD` was in `NT_STATUS_DELETE_PENDING`. The NAS still enumerated the
entry, but neither the CIFS client nor a separate SMB client could open it.
No user-owned process had an open descriptor for the path. The likely
remaining owner was a stale CIFS session handle or another NAS client.
Changing the visible ACL did not resolve the server-side handle state.

### Git metadata restoration

Git metadata was restored on 2026-07-13 using Git's supported separate
Git-directory layout:

- immutable recovery copy:
  `/home/arodrigues/.local/share/aps-database-git-recovery-20260713-safe`;
- active local metadata:
  `/home/arodrigues/.local/share/gitdirs/APS_Database.git`;
- original temporary recovery:
  `/tmp/aps_database_git_recovery_20260713`; and
- preserved broken CIFS metadata:
  `/home/arodrigues/APS_Database/APS_Database.git.invalid-20260713-cifs`.

The broken directory was renamed through a direct authenticated SMB operation,
which bypassed the stale Linux CIFS namespace. The working tree now contains a
regular `.git` pointer file:

```text
gitdir: /home/arodrigues/.local/share/gitdirs/APS_Database.git
```

Ordinary Git commands no longer need `--git-dir` or `--work-tree`.
Post-restoration verification established:

- `git rev-parse --is-inside-work-tree`: `true`;
- branch/SHA: `architecture-foundation-2026-07` at
  `8baa144a6b7fad272f8254b901175a231b2c8634`;
- `git fsck --full`: successful, with 112 unreachable historical objects and
  no connectivity failures;
- reversible `git update-ref` create/read/delete transaction: successful;
- the recovered index reproduces the expected modified/untracked inventory;
  and
- read-only remote access: `origin/HEAD` and `origin/master` both resolve to
  `8baa144a6b7fad272f8254b901175a231b2c8634`.

This is a healthy layout for this deploy host and avoids Git lock and rename
operations on CIFS. The pointer contains an absolute host-local path, so the
NAS working tree is not independently portable to another machine. A new host
must clone the repository normally or create its own separate Git directory;
it must not copy this pointer and assume the local path exists.

The safe and broken-metadata copies must be retained until the release commits
are pushed to a durable remote. The active metadata directory is now the only
copy that should receive new commits; the `/tmp` and `-safe` copies are
recovery snapshots and must not be used concurrently.

### Locked Claude worktree disposition

The locked `viewer-overhaul-plan` worktree contained one uncommitted query
change in the historical
`data_processing_scripts/plot_damage_signature_delta_3d.py`. The exact added
columns, ranked-view source, pool-size calculation, and top-10 filter are
already present in
`src/aps/viewers/plot_damage_signature_delta_3d.py`. The owner edit is therefore
preserved in the packaged module. The stale worktree will be removed only
after the corresponding packaged source is committed and verified.

## Release A implementation log

Status: **repository implementation complete and verified; production adoption
is intentionally deferred to Release B**.

Completed foundation changes:

- centralized environment parsing, secret validation, redacted summaries,
  database connections, and mounted-directory preflight checks in
  `aps.config` and `aps.db_config`;
- adopted the database facade across active ingestion and seed entry points;
- added commit/dirty-tree source fingerprints and production refusal for
  migrations, model builds, and nightly runs;
- added checksum-protected forward migrations with a mandatory exact adoption
  cutoff and a separate repeatable-model ledger;
- moved avalanche administration DDL out of Flask requests into migration
  `031_flask_avalanche_admin.sql`;
- replaced the imperative nightly step list with a dependency-validated
  manifest and persistent run/step transitions while retaining backup, lock,
  and service-health responsibilities in the shell wrapper;
- hardened service identity, group access, preflight validation, umask, and
  privilege boundaries;
- added a strict timeout/error-aware Superset transport without coupling it to
  Release C presentation policy;
- added constrained packaging, tracked CI, offline architecture tests, and a
  separate disposable-PostgreSQL integration job; and
- kept Release C-only legacy CV/DPT, proxy comparison, chart-description, and
  viewer/dashboard changes out of the foundation boundary.

Repository verification:

- exact staged-tree default suite: 310 collected, 10 deselected, 300 passed;
- disposable PostgreSQL 15 tier: 4 selected, 4 passed;
- Ruff architecture scope: passed;
- `pip check`, `py_compile`, `git diff --check`, and shell syntax: passed;
- `systemd-analyze verify` for tracked services/timer: passed; and
- CLI migration planning, source provenance, and release-status smoke checks:
  passed.

These results establish repository and disposable-service behavior. They are
not evidence that the existing production database, backups, deployment
checkout, or systemd schedule has been adopted.

## Release B implementation log

Status: **in progress; production mutation paused at the durability and
privilege gates**.

Release B discovery and safety actions completed on 2026-07-14:

- confirmed the deployed checkout is clean at `6ac2594` and that this commit
  is an ancestor of Release A (`27d882f`), so the planned deployment is a
  true fast-forward and preserves the deployed phenotype-viewer link fixes;
- verified PostgreSQL 15, the APS database, and the Superset metadata
  database are healthy and reachable without changing them;
- verified the release backup pair and its recorded SHA-256 checksums:
  `mosfets-releaseB-20260713T160523Z.dump` (668,296,277 bytes) and
  `superset_metadata-releaseB-20260713T160523Z.dump` (24,650,406 bytes);
- reviewed the prior disposable-restore fingerprints. Restored APS counts
  (`baselines_metadata=5275`, `device_library=43`,
  `irradiation_campaigns=8`, `irradiation_runs=20`) and restored Superset
  counts (`dashboards=13`, `datasets=84`, `slices=294`, `users=1`) match the
  current production catalog fingerprints;
- confirmed production has no `aps_forward_migrations`, `aps_model_builds`,
  or `pipeline_runs` relation yet, so first adoption must use the reviewed
  exact historical cutoff and cannot be treated as an ordinary upgrade;
- identified that the legacy user-level nightly timer was still enabled and
  had failed every night through 2026-07-14 on the same missing
  `stress_proxy_distance_settings.phenotype_mismatch_distance` column; and
- disabled and stopped that user timer before its next 03:00 trigger. It is
  now `disabled` and `inactive`, preventing another known-failing unattended
  mutation while adoption is incomplete.

No production schema, model, application checkout, container, Superset
metadata, or system service was changed during this discovery. Disabling the
legacy timer was the only production-state action and is reversible.

Remaining Release B gates:

1. The user explicitly approved pushing private repository changes to
   `git@github.com:arodrigues-APS/APS_Database.git` on 2026-07-14. The
   execution safety layer still prohibited the push, so the operator must run
   the documented one-line push command directly. Release A remains local
   until that command succeeds.
2. This session has no non-interactive sudo authorization. The guarded
   `scripts/bootstrap_release_b_systemd.sh` procedure now provisions
   `/etc/aps/aps.env`, preserves old unit files, installs the system units,
   reloads systemd, and proves the timer is disabled. It must be run by an
   authorized operator; no password will be requested or stored by this
   implementation process.
3. After durability and configuration are established: fast-forward `/opt`,
   baseline the reviewed migration prefix through
   `026_irradiation_energy_windows.sql`, apply migration 031, build the
   repeatable proxy model, run production smoke checks, and execute one
   attended nightly shadow run.
4. Enable the new timer only if the shadow run, ledgers, backups, logs,
   artifacts, mounts, and failure recorder all verify successfully.

## Release C implementation log

Status: **not started; blocked on Release B acceptance**.

## Verification ledger

Each verification entry must include the command class, environment, result,
and whether it was offline, disposable-service, or production evidence.

| Date | Scope | Evidence | Result |
|---|---|---|---|
| 2026-07-13 | Pre-implementation offline suite | 315 collected, 6 production-smoke tests deselected, 309 passed; `pip check`, `git diff --check`, and shell syntax passed | Baseline established; not production evidence |
| 2026-07-13 | Git metadata recovery | Direct SMB diagnosis reported `NT_STATUS_DELETE_PENDING`; separate local Git directory installed; ordinary discovery, full fsck, reversible ref transaction, status, and remote read all passed | Git healthy on this deploy host; recovery branch remains local |
| 2026-07-13 | Release A exact staged-tree offline suite | 310 collected, 10 deselected, 300 passed; Ruff, `pip check`, compilation, diff whitespace, shell syntax, systemd unit parsing, and CLI smoke checks passed | Foundation repository gates passed without relying on unstaged Release C files |
| 2026-07-13 | Release A disposable PostgreSQL 15 | 4 integration tests passed against per-test databases: blank/idempotent migration lifecycle, exact historical adoption plus 031, checksum/rollback/retry, and real model/nightly ledgers | Database lifecycle gate passed without production access |
| 2026-07-14 | Current mixed-tree offline regression | 325 collected, 10 integration/production-smoke tests deselected, 315 passed; focused Ruff architecture scope passed | Release A, the Release B bootstrap, and the still-uncommitted Release C working tree remain green; not production evidence |
| 2026-07-14 | Release B production discovery | Release backups passed recorded SHA-256 checks; prior restored fingerprints matched current APS and Superset catalog counts; production ledgers absent; deployed `6ac2594` confirmed ancestor of `27d882f` | Adoption boundary established without database mutation |
| 2026-07-14 | Nightly safety gate | Legacy user timer had repeated failures through 2026-07-14; timer disabled and stopped before next trigger; final state `disabled`/`inactive` | Known-failing unattended mutation prevented pending shadow verification |
| 2026-07-14 | Release B operator bootstrap | Shell syntax and systemd unit verification passed; non-root execution refused before mutation; 17 focused tests and Ruff passed; boundary test proves the script contains no start, restart, or enable operation | Root bootstrap is reviewable and fail-closed; operator execution remains pending |

## Open release gates

- The local recovery branch has no upstream. Release commits must be pushed to
  a durable remote before a release is accepted. Approval has been granted,
  but the execution safety layer requires the repository owner to run the
  documented push command directly.
- The local safe copy and preserved broken CIFS metadata must be retained until
  those release commits are present on the remote.
- Data and Superset backups must be restored successfully before production
  mutation.
- Environment files, credential rotation, database adoption, `/opt`
  deployment, service installation/restart, smoke checks, nightly shadowing,
  and timer enablement remain unapplied. Backup checksum and disposable-restore
  evidence is present and catalog fingerprints match; a final release manifest
  still needs to bind that evidence to the deployed SHA.
