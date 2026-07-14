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

The active metadata directory is now the only copy that should receive new
commits; the `/tmp` and `-safe` copies are recovery snapshots and must not
be used concurrently. Release A and the Release B bootstrap became durable on
the private remote on 2026-07-14 when
`architecture-foundation-2026-07` was pushed through commit `46629d9`.
The safe and broken-metadata copies must still be retained until the Release C
code and implementation-record commits are also present on the remote.

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

Status: **in progress; source durability is established, and production
mutation is paused at the privilege/configuration gate**.

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

1. This session has no non-interactive sudo authorization. The guarded
   `scripts/bootstrap_release_b_systemd.sh` procedure now provisions
   `/etc/aps/aps.env`, preserves old unit files, installs the system units,
   reloads systemd, and proves the timer is disabled. It must be run by an
   authorized operator; no password will be requested or stored by this
   implementation process.
2. After configuration is established: fast-forward `/opt`,
   baseline the reviewed migration prefix through
   `026_irradiation_energy_windows.sql`, apply migration 031, build the
   repeatable proxy model, run production smoke checks, and execute one
   attended nightly shadow run.
3. Enable the new timer only if the shadow run, ledgers, backups, logs,
   artifacts, mounts, and failure recorder all verify successfully.

## Release C implementation log

Status: **repository implementation and production-sized disposable
verification complete; production release remains blocked on Release B
acceptance**.

Release C was reviewed and implemented without applying its schema, Superset
metadata, application code, or services to production. The resulting
repository changes were committed as `87b43f9`
(`Add evidence-safe proxy and dashboard comparisons`) and are:

- corrected the v3 score so missing candidate destruction-boundary evidence
  uses the declared failure-fraction overlap penalty instead of silently
  reusing terminal-energy overlap, and made the comparison decomposition use
  the same term;
- added a complete v1/v2/v3 winner-union model and CSV export that retains
  official winners even when a v1 winner falls outside v2's materialized
  top-10, exposes rank availability explicitly, records full shared source
  provenance, and labels the evidence as screening-only;
- added the comparison export to the nightly DAG and made the interactive
  viewer declare all of its upstream exports/models rather than depending on
  tuple order or accepting stale CSV contracts;
- registered the legacy CV/DPT compatibility views as the
  `legacy-cv-dpt` repeatable model, with a strict
  `APS_ENABLE_LEGACY_CV_DPT` activation setting that defaults to false and
  refuses a build before taking the model lock or writing a ledger row;
- kept dashboard builders out of DDL ownership and added contract tests for
  dataset provenance, chart/filter scope, and comparison semantics;
- added shared non-proxy dashboard presentation support while preserving
  existing dashboard identity/slug behavior;
- made chart-description backfill and portfolio reconciliation dry-run by
  default, verify both dashboard ID and title before mutation, and refuse the
  entire apply if any identity is inconsistent; and
- designated dashboards 14, 16, 28, and 33 as title-verified archive targets.
  Dashboard 33 is included because production currently publishes the legacy
  CV/DPT presentation even though the compatibility model is disabled by
  policy.

Production read-only inspection found that the live database already contains
relations shaped like the in-progress v3/comparison work and that all 2,754
comparison rows lack curated truth. That is useful compatibility evidence but
is not proof of how those objects were deployed and is not a substitute for
the model ledger. No Release C production mutation was performed.

The exact Release C code content later committed as `87b43f9` was tested
against a full restore of
`mosfets-releaseB-20260713T160523Z.dump` in an isolated PostgreSQL 15
container:

- the restore completed successfully and reproduced 5,275 baseline metadata
  records, 43 device-library rows, 8 irradiation campaigns, 20 irradiation
  runs, and the expected empty avalanche campaign set;
- a from-source `proxy-analytics` build succeeded with checksum
  `fcf6204f3a8d`, producing 8,190 v3 candidates and 2,754 comparison rows
  across 1,300 targets;
- the maximum absolute error between the comparison component decomposition
  and the official v3 distance was exactly zero, with zero winner-contract
  violations;
- all 1,466 comparison rows with an available v3 rank used the explicit
  missing-boundary penalty consistently, rather than terminal-energy reuse;
- the target transitions were 1 `all_three_same`, 481
  `v2_winner_unavailable`, 182 `v3_follows_v2`, and 636
  `v3_selects_third_candidate`;
- the disabled legacy build refused with exit code 2 and no build record; an
  explicitly enabled disposable build then succeeded with exact source/view
  parity for CV (7,236), DPT waveforms (1,273,141), and DPT switching metrics
  (189); and
- the comparison exporter wrote exactly 2,754 data rows with one consistent
  full Git revision, dirty-state flag, source fingerprint, and Git-available
  flag.

This establishes database compatibility and internal scientific consistency
for the candidate. It does not establish a production release: Release C must
be pushed, deployed only after Release B, applied through the registered
model/dashboard paths, and verified against the release manifest before it can
be called successful.

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
| 2026-07-14 | Private remote durability | Operator push created and configured `origin/architecture-foundation-2026-07`; local and remote both resolved to `46629d9b5e2fab0740331755e6e17ddd6c025f60` | Release A and Release B bootstrap are durable; later Release C commits still require a follow-up push |
| 2026-07-14 | Release C production read-only audit | Existing proxy-v3/comparison catalogs and Superset dashboard IDs/titles inspected; 2,754 comparison rows all reported `no_curated_truth`; dashboard 33 was published despite the disabled legacy policy | Compatibility and policy drift identified without production mutation |
| 2026-07-14 | Release C full production-snapshot restore/build | PostgreSQL 15 restore succeeded; source fingerprints matched; proxy model build succeeded; 8,190 v3 rows, 2,754 comparison rows/1,300 targets, exact component equality, zero winner violations; gated legacy build refusal and explicitly enabled parity build both passed | Production-sized database gate passed in a disposable service; not deployment evidence |
| 2026-07-14 | Release C exporter and PostgreSQL lifecycle | Comparison CSV contained 2,754 rows plus header and consistent full provenance; 4 explicitly selected integration tests passed | Export provenance and real lifecycle behavior verified against disposable PostgreSQL |
| 2026-07-14 | Release C repository regression | 327 default tests and 36 subtests passed, 10 integration/production-smoke tests correctly deselected; all changed Python passed Ruff; the complete source/test tree compiled; `git diff --check` passed | Release C change scope is green; repository-wide Ruff still has 59 pre-existing errors in untouched legacy modules |

## Open release gates

- Release A and the Release B bootstrap are on the private remote at
  `46629d9`. Release C code commit `87b43f9` and the implementation-record
  commit must be pushed before deployment, and the local safe copy plus
  preserved broken CIFS metadata must be retained until that succeeds.
- The guarded sudo bootstrap must create and validate `/etc/aps/aps.env` and
  install the system units while proving the nightly timer remains disabled.
- Credential provisioning/rotation, exact migration adoption, `/opt`
  fast-forward deployment, proxy model build, Flask/service smoke checks, and
  one attended nightly shadow remain unapplied.
- The timer may be enabled only after the shadow run verifies ledgers,
  backups, logs, artifacts, mounts, failure recording, and service identity.
- Release C must then be deployed as a separate reviewed commit, its registered
  models built, its Superset dry-runs reviewed before explicit apply, and its
  dashboard identities and screening-only evidence policy verified.
- A final release manifest must bind the backup checksums, deployed Git SHA,
  migration/model ledger rows, Superset reconciliation result, smoke results,
  and rollback point. Only that evidence closes Release C and permits a
  production-success report.
