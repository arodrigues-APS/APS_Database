# Repository architecture & best-practices review — 2026-07-06

Scope: the full `APS_Database` repository — file organization, packaging, configuration,
schema/pipeline architecture, testing, deployment, and repo hygiene — plus a proposal for
how the codebase would be structured if written from scratch today, and a pragmatic,
thesis-aware adoption path.

Method: read-only inspection of the working tree, git history/tracking state, the nightly
and deploy scripts, import-dependency mapping across all Python modules, and LOC/duplication
analysis. No code was changed.

Follow-up method (2026-07-11): independent inspection of the post-reorganization working
tree, targeted AST/LOC/dependency analysis, the full test suite, read-only PostgreSQL
catalog queries, and read-only comparison of the source checkout, the active `/opt`
checkout, and installed systemd units. The follow-up changed this review document only;
it did not modify application code, the database, Superset, deployment state, or services.

---

## Status update — 2026-07-06 (same day, post-review)

Decisions and implementation state:

- **F1 (docs tracking): declined by owner** — `docs/` stays untracked deliberately; it is
  personal notes. Mitigating context found during implementation: the repo has a GitHub
  remote (`arodrigues-APS/APS_Database`) and `master` is fully pushed, so code history is
  backed up off-NAS; `docs/` remains the one single-copy area by choice.
- **Phase 0 implemented** (commit `0d85125`): `out/` fully untracked (files kept on disk;
  `deploy_to_opt.sh` preserve logic verified compatible), root `default` deleted after
  confirming `/etc/nginx/sites-enabled/` uses a root-owned copy of the `aps_database` site
  (not the repo file), `.gitignore.save` deleted, `.gitignore` updated (`*.egg-info/`,
  `.pytest_cache/`, comments). Stale worktrees removed: the dirty
  `mech-energy-phase4` worktree's uncommitted edits were first committed onto its
  branch `worktree-mech-energy-phase4` (nothing lost; branch kept), the clean
  `/tmp/aps-deploy.*` deploy leftover removed.
- **Dead code removed** (commit `cb53072`): `DatabaseScript.py` + `datasheetgm/rdson/vth.py`
  (2,857 LOC), after re-verifying zero code/SQL consumers.
- **Phase 1 implemented** (commit `e35ca14`): `pyproject.toml` (installable package,
  extras `[server]`/`[pipeline]`/`[ml]`/`[dev]`, pytest+ruff config), root `conftest.py`
  (tests pass from any CWD; per-file path hack removed), `requirements.txt` slimmed to a
  pointer at the extras (drops `pathlib==1.0.1` backport, `ipython`/`jedi` from runtime),
  `.env.example` documenting all `APS_*`/`SECRET_KEY` variables. Verified: 239 tests green
  before and after each commit; `pip install -e . --no-deps` builds in a scratch venv.
  Production venv/systemd untouched.
- **Not done (unchanged from plan):** secret rotation / removal of committed defaults
  (needs coordinated env provisioning on the server), and all of Phase 2 (deferred until
  after the thesis snapshot/freeze tag).

## Status update — 2026-07-09 (Phase 2 started early, per owner directive)

Owner decided not to wait for the thesis freeze tag. Implemented and pushed:

- **Tag `pre-reorg-2026-07`** at `e8058ee` (pushed): thesis-citable stable tree divider
  between the pre- and post-restructure layouts.
- **Migration ledger** (commit `fb280c5`): `apply_schema` now records every executed file
  in a `schema_migrations` table in the same transaction — one row per (filename, content
  version) with sha256; unchanged re-applies bump `last_applied_at`. `schema_status()` +
  CLI: `python -m aps.common [--apply] [--include-pipeline ...]` reports
  `in_sync / edited_since_apply / never_recorded / missing_file` per file. Directly
  addresses F6's "was 025 applied live?" failure mode. psql applies still bypass the
  ledger (documented in `schema/README.md`). **Deliberate deviation from §4:** schema
  files were NOT renumbered — 020–029 are load-bearing references in the thesis notes
  and memory; the ledger supplies the audit trail instead.
- **Phase 2 re-org** (commit `8baa144`): `data_processing_scripts/` (47 modules) →
  installable `aps` package under `src/`, layered `ingest / seeds / enrich / proxy / ml /
  superset / viewers / exports` per §5's mapping (pure relocation, no renames/splits;
  every file ≥93% rename similarity so `git log --follow` works). New `aps/paths.py`
  (REPO_ROOT / SCHEMA_DIR / OUT_ROOT) replaces 13 per-file `Path(__file__).parent.parent`
  chains and 6 CWD-relative `out/` paths — scripts are now CWD-independent. All three
  import styles collapsed to absolute `aps.*`; per-file `sys.path` hacks deleted.
  Rewired in the same commit: nightly script (`PYTHONPATH=src`, `cd` repo root, `-m`
  invocations, viewer artifact at `<repo>/out/`), `deploy_to_opt.sh` status paths,
  `server.py` (adds `src/` to `sys.path` for uwsgi), `pyproject.toml` (src layout,
  v0.2.0), root `conftest.py`. `one_time_scripts/` → `archive/`.
  Verified: 268 tests + 36 subtests from repo root and `$HOME`; all 56 modules
  import from a foreign CWD; editable install rebuilt in `~/aps_venv`; `bash -n` clean.
- **Deploy state:** `/opt/aps_database/APS_Database` still runs the OLD layout and is
  fully self-consistent until `deploy_to_opt.sh` is run. Deploy checklist when ready:
  (1) `scripts/deploy_to_opt.sh master`; (2) restart the uwsgi service (`server.py`
  changed); (3) optional `/opt/aps_database/venv/bin/pip install -e ... --no-deps`
  (nightly uses PYTHONPATH, so not strictly required); (4) stale
  `data_processing_scripts/out/` leftovers under /opt are harmless — the first nightly
  regenerates everything under repo-root `out/` and publishes from there.
- **Left untouched:** locked worktree `.claude/worktrees/viewer-overhaul-plan` (has an
  uncommitted owner edit to `plot_damage_signature_delta_3d.py`; note that file now
  lives at `src/aps/viewers/` on master, so that change must be ported by hand).
- **Phase 2 remainder (open):** Superset DSL extraction (§6.2), viewer template/JS split
  (§6.3), `pipelines/nightly.py` orchestrator (§6.4a), `server.py` blueprints + ML
  monolith split (§6.5), secret rotation (F8).

## Independent follow-up review — 2026-07-11

### Executive verdict

The original review was directionally correct, and the Phase 0 cleanup plus the physical
move to `src/aps/` were useful. The work is nevertheless **not architecturally complete**.
The current state is best described as:

- **Phase 0: substantially complete**, subject to the owner's accepted decision to keep
  most of `docs/` ignored and one still-locked Claude worktree.
- **Phase 1: partially complete.** Package metadata and import paths exist, but runtime
  dependency installation, hard-coded environment paths, committed secrets, an unwired
  `.env.example`, no dependency lock, and no CI mean the package is not yet the
  reproducible execution boundary the phase intended to create.
- **Phase 2: directory move complete; responsibility-boundary work mostly open.** The
  subpackage names now describe the desired layers, but DDL ownership, orchestration,
  database transformations, Flask startup, Superset automation, viewer assets, and the ML
  monoliths still cross those boundaries.
- **Production rollout: not complete.** The active Flask service uses an `/opt` checkout
  14 commits behind the source branch, while the intended APS nightly systemd unit/timer
  is not installed. This is a release-management problem, not just deferred refactoring.

The most important correction to the original plan is that the database needs **two
different mechanisms**, not one folder called “migrations”:

1. forward-only structural migrations, applied once and checksum-protected; and
2. repeatable derived models (views/materialized views), with declared dependencies,
   refresh policy, timing, row-count checks, and their own build ledger.

Treating multi-gigabyte materialized-view rebuilds as startup migrations is the largest
architectural risk missed by the first review.

### Evidence snapshot

This snapshot includes the owner's in-progress working tree; it is not a claim that the
uncommitted dashboard work is release-ready.

- Source HEAD: `8baa144` on `master`, matching `origin/master`.
- Working tree: 14 tracked files modified and 9 untracked files, including
  `schema/030_dynamic_characterization.sql`, its dashboard builder, shared Superset
  support/reconciliation modules, an export, and tests. The existing changes were not
  altered by this review.
- Current `src/aps/`: 62 Python files, approximately 42,394 LOC. Largest modules are
  `create_interactive_damage_signature_viewer.py` (4,143 LOC),
  `ml_post_iv_physical_prediction.py` (3,343),
  `create_proxy_readiness_dashboard.py` (2,874), and
  `ml_sc_irrad_equivalence.py` (2,234).
- Tests: 280 collected and 280 passed in 2.09 s on the deploy server's Python 3.12
  environment. This result is not hermetic: six tests in
  `test_stress_context_figure1b.py` query the live default PostgreSQL database and assert
  production row-count floors. They are not marked as integration tests.
- Static-quality gate: Ruff is declared in the `dev` extra and configured, but is not
  installed in the mandated `~/aps_venv`; there is no CI or pre-commit configuration.
- SQL on disk: 8,983 lines in `schema/*.sql` (including the in-progress 030 file), plus
  32 schema-defining string blocks / approximately 3,376 DDL lines embedded in
  `src/aps/*.py`. DDL therefore still has multiple owners.
- Live PostgreSQL catalog (point-in-time, read-only): `baselines_metadata` has 77 columns;
  the public schema has 735 base tables and 20 materialized views; base tables occupy
  about 6.9 GB and materialized views about 14 GB. Notable relations are
  `stress_proxy_candidate_ranked_view` (8.6 GB total),
  `iv_prediction_points` (4.6 GB), `iv_prediction_dashboard_curve_view` (4.6 GB),
  and `iv_prediction_dashboard_summary_view` (1.0 GB). Sizes will change with data.
- Active deployment: systemd reports `server.service` active with working directory
  `/opt/aps_database/APS_Database`; that checkout is clean at `6ac2594`, 14 commits
  behind `8baa144`. The repository copy of `server_config/server.service` instead names
  the home checkout, so it is not the deployed truth.
- Scheduler: `aps-nightly.service` is not installed, no APS timer is listed, and the
  `arodrigues` user has no crontab. A root-owned or differently named scheduler was not
  established by this audit, so the narrow conclusion is that the repository's intended
  systemd nightly path is not active.
- Documentation: this review itself is ignored by `.gitignore` because `docs/` remains
  owner-designated personal material. Updating it will not appear in normal `git status`
  or be preserved by a normal commit unless explicitly force-added; that is an accepted
  ownership decision, not an accidentally “completed” F1.

### Reassessment of the original findings

| Finding | 2026-07-11 state | Follow-up assessment |
|---|---|---|
| F1 documentation | Accepted risk, not fixed | The owner explicitly declined default tracking. Record this as accepted risk and maintain a separate backup/export routine; do not keep reporting it as an implementation task. |
| F2 god-folder/imports | Structurally improved | `src/aps/{ingest,seeds,enrich,proxy,ml,superset,viewers,exports}` and absolute imports are real gains. The root Flask app and fingerprint script still add `src` to `sys.path`, the nightly exports `PYTHONPATH`, and cross-layer imports show the boundaries are descriptive rather than enforced. |
| F3 dead code/cruft | Mostly complete | The 2,857-LOC island and tracked generated output were removed. The locked `.claude/worktrees/viewer-overhaul-plan` remains and contains an owner edit that still needs an explicit disposition. |
| F4 dashboard duplication | Partially in progress | The uncommitted `nonproxy_dashboard_support.py` centralizes descriptions and width-aware tab layout. Chart-parameter factories, native filters, chart catalog/spec structure, association logic, and failure handling remain duplicated. The low-level API client still imports PNG export code and now lazily imports presentation policy, so dependency direction is still inverted. |
| F5 monoliths | Open | The largest viewer grew from roughly 3,064 to 4,143 LOC. Both ML monoliths and `server.py` remain unsplit. Moving them did not reduce mixed responsibilities. |
| F6 SQL state | Only partially addressed | The ledger records only files executed through `common.apply_schema`. Major callers read and execute 025/028/029 directly, the new 030 builder does the same, and 3,376 DDL lines remain embedded in Python. The ledger therefore cannot answer which version of every live database object was built. |
| F7 bash orchestration | Open | The 300+ line shell script still owns ordering, infrastructure updates, backups, publishing, retention, and error policy. There is no run/step ledger, resume-from-stage behavior, or machine-readable degraded result. |
| F8 secrets | Open and release-blocking | Production-like DB/Superset/Flask/Superset-session secrets remain committed as defaults. The data DB port is published on all host interfaces by Compose. Rotation and fail-fast secret provisioning were not completed. |
| F9 dependencies | Partially addressed | Extras replaced the 2022 freeze, but most dependencies remain unlocked, uWSGI is outside the declared server environment, Ruff is absent from the project venv, and nine modules can invoke `pip install` at runtime. |
| F10 testing/quality | Partially addressed | Import setup and additional unit tests improved the suite. Ingestion, Flask routes, real PostgreSQL migration behavior, the orchestration DAG, and end-to-end Superset updates still lack isolated tests; one production-DB test file is mixed into the default unit suite; no CI runs any gate. |
| F11 repo/data co-location | Open | Code remains in the NAS-backed tree while the active service runs a separate `/opt` checkout. The practical problem is now split-brain release state as well as CIFS reliability. |

### Findings missed or understated by the first review

#### M1 — Deployment and scheduling have no reconciled source of truth (critical)

The review described deployment as a future checklist, but did not verify whether the
checked-in units were installed or whether the active service used the same checkout.
They do not:

- active Flask code is the clean `/opt` checkout at `6ac2594`;
- source development is at `8baa144` plus a large dirty working tree;
- the checked-in server unit points to the home checkout, while the installed unit points
  to `/opt`;
- the checked-in APS nightly service/timer is not installed; and
- `deploy_to_opt.sh` fast-forwards code but deliberately does not install dependencies,
  apply a release manifest, restart services, install units, or run post-deploy smoke
  checks.

This permits the web app, database objects, Superset metadata, and source tree to advance
independently. A successful source commit is not evidence of a successful release.

#### M2 — The migration ledger is an audit log for one code path, not a migration system

`common.apply_schema` always executes eligible files, then records/touches their
checksums. It does not skip an already-applied migration, reject edits to an immutable
migration, coordinate concurrent appliers, or own embedded/direct SQL. Examples outside
the ledger include:

- `extract_single_event_effects.ensure_proxy_readiness_views()` executing 025 and 028;
- `create_proxy_readiness_dashboard.apply_proxy_schema()` executing 025, 028, and 029;
- `apply_mechanistic_energy_proxy.apply_schema()` executing 025, 028, and 029;
- the ML SC-equivalence module carrying a roughly 1,750-line view bundle;
- ingestion and seed modules carrying another roughly 1,600 lines of DDL; and
- the in-progress dynamic-characterization builder applying 030 directly.

The same mechanism also applies SQL from Flask import/startup. Forward migrations,
repeatable derived models, and application startup must be separated.

#### M3 — A removed “dead” ingestion path still owns live data now being reused (critical)

The new `schema/030_dynamic_characterization.sql` reads `cpvd`, `dptgraphs`, and
`dptslopes`. No current ingestion module produces those tables. Git history shows their
only producer was the removed `DatabaseScript.py`, which also created hundreds of
measurement-specific tables and began by dropping every table in `public`.

The live database still has 735 base tables, with names such as per-device/per-condition
CV and DPT captures. Building a new dashboard over the aggregate remnants makes the
dashboard dependent on live-only historical state: it works on this server but cannot be
recreated from a fresh checkout/database. The old script must **not** be restored as-is;
the decision is either:

1. write a safe, incremental CV/DPT importer into canonical tables, with source-file
   hashes and parity checks against the frozen legacy aggregates; or
2. explicitly designate the legacy tables as a read-only snapshot, document their
   provenance/backup, and keep 030 out of core startup/nightly guarantees.

Until one is chosen and tested, 030 should not be an unconditional core schema file.

#### M4 — Materialized-view cost and rebuild topology dominate the database architecture

The first review counted SQL locations but did not inspect physical relations or how often
they rebuild. The current nightly script calls three stages that execute 025/028:

1. single-event extraction;
2. Proxy Readiness dashboard creation; and
3. the optional mechanistic-proxy apply step.

Thus 025 and 028 can be dropped/recreated three times in one nominal run, while 029 can be
rebuilt twice. File 025 alone is 4,099 lines and owns eleven materialized views, including
the observed 8.6-GB `stress_proxy_candidate_ranked_view`. This creates unnecessary
runtime, locks, temporary disk amplification, and multiple failure points. Dashboard
builders should consume prepared data; they should not rebuild the analytical warehouse.

#### M5 — The package can mutate its own environment at runtime

Nine ingestion/seed/enrichment modules catch `ImportError` and run
`python -m pip install ...`. This defeats the package extras, makes a production run
network-dependent, can install a different version than the tested one, and may modify a
shared venv while another process is using it. Missing dependencies must fail during a
preflight/install step, never be repaired from an imported application module.

Configuration is similarly incomplete:

- `.env.example` explicitly notes that application code does not load it;
- checked-in systemd units do not declare an `EnvironmentFile`;
- Superset Compose expects a separate `superset/.env`;
- baselines and SC ingestion still hard-code personal absolute source paths; and
- committed defaults silently select the production-like database and credentials.

#### M6 — “280 tests pass” currently includes the live production database

Six default tests connect through `aps.db_config.get_connection()` and assert minimum
live row counts. They passed here because this is the deploy server and its database is
available. On a clean developer machine or CI runner, the default suite would fail or
could accidentally target the wrong database. Unit, integration, and production smoke
checks need separate markers, credentials, and commands. Migration tests must use a
disposable PostgreSQL database; fake cursors alone cannot validate PostgreSQL DDL,
transactional behavior, locks, dependencies, or materialized views.

#### M7 — Flask import performs infrastructure work

Importing `server.py` currently:

- resolves configuration relative to the current working directory;
- scans `/data/www` to pre-warm a file cache; and
- connects to PostgreSQL and applies schema files, failing application boot if this work
  fails.

This couples process start/reload to NAS latency and database mutation, can run more than
once under a multi-process WSGI server, and makes app tests require infrastructure merely
to import the module. An app factory should build routes and dependencies; an explicit
release command should migrate; a background/cache service should own scanning.

#### M8 — Superset automation can fail while returning apparent success

The shared client has no HTTP timeouts. Dataset refresh calls ignore response failures;
a failed chart update is logged but returns the existing chart id; a failed dashboard
update returns the existing dashboard id; and many chart-association failures are printed
without raising. The shell therefore sees exit code zero and can log a completed nightly
even when Superset is partly stale. The client also mixes transport, presentation policy,
PNG export, and dashboard ownership.

The in-progress shared layout/description module is useful, but it is only the beginning
of F4. A strict client/service layer must return typed results or raise; a declarative
dashboard spec layer should own chart/filter/layout definitions; reconciliation should
verify the resulting remote state.

#### M9 — Nightly data processing and infrastructure upgrades are coupled

Every nominal nightly run performs `docker compose pull` and recreates PostgreSQL,
Redis, Superset, and workers before ingestion. Major-only image tags such as
`postgres:15` and `redis:7` are mutable, so an upstream image change and a data-model
change can occur in the same failure window. Backups are a strong safeguard, but upgrades
should be an explicit maintenance/release action with a restore rehearsal, not an
unconditional prelude to ETL.

There is also no persistent pipeline-run/step record containing code SHA, input snapshot,
start/end times, status, row counts, schema/model versions, artifact paths, and failure
reason. The text log and standalone fingerprint script help humans, but do not make a run
auditable or resumable.

#### M10 — Documentation describes history better than current operation

`Readme.md` still documents only the 2022 Flask file-search app and tells users to run
`pip install -r requirements.txt`. It does not explain the PostgreSQL pipeline,
Superset, package commands, schema/model lifecycle, test tiers, the two checkouts, or
release/rollback. The architecture review's pre-reorganization LOC tables and path
mapping are useful history but should not be read as current inventory; this follow-up and
the completion plan below supersede those status assumptions.

---

## 1. What this repository actually is

Two projects share one repo, stacked in time:

1. **2022 (11 commits, Oct–Nov):** a small Flask front-end to keyword-search the lab's
   measurement file share (`server.py` + `data_scraping.py` + `templates/`), deployed with
   uwsgi/nginx. `Readme.md` still describes only this.
2. **2026 (66 commits, Feb–Jul):** the MSc-thesis system — PostgreSQL database, nightly
   ingestion pipeline, physics/proxy engine, ML models, Superset dashboard builders, and
   interactive 3D HTML viewers — nearly all of it added into `data_processing_scripts/`.

Current code volume (excluding the stale `.claude/worktrees` copy):

| Cluster (in `data_processing_scripts/`) | Files | LOC |
|---|---|---|
| Superset dashboard builders + API client + PNG export | 11 | 10,247 |
| Ingestion + extraction (`ingestion_*`, `extract_*`, `common`, logbooks, promote) | 10 | 9,319 |
| Proxy/physics engine (`mechanistic_energy_proxy`, `calibrate_*`, `apply_*`, dose, energy windows, depletion model, exports) | 10 | 5,723 |
| ML pipelines (`ml_post_iv_physical_prediction`, `ml_sc_irrad_equivalence`) | 2 | 5,577 |
| Interactive HTML viewers + matplotlib plots | 5 | 5,178 |
| Legacy island (`DatabaseScript.py`, `datasheetgm/rdson/vth.py`) | 4 | 2,857 |
| Seeding (`seed_*`) | 5 | 2,710 |
| Config (`db_config.py`) | 1 | 37 |
| **Total `data_processing_scripts/`** | **52** | **42,107** |

Plus: `server.py` (935 lines, 24 routes), `tests/` (18 files, 3,053 LOC, ~239 test
functions), `schema/` (14 SQL files + README), `scripts/` (nightly/deploy/fingerprint),
`superset/` (docker-compose + config), `server_config/` (systemd + nginx), `docs/`
(35 top-level items incl. 270 MB of papers), `out/` (194 MB generated artifacts, 8 tracked).

The name `data_processing_scripts` covers one of at least eight distinct subsystems living
inside it. That is the core structural finding; most of the rest follows from it.

---

## 2. What is working well (worth preserving in any restructure)

These are genuinely good practices already in place — several are better than typical
research-code baselines:

- **Central connection/config shim** — `db_config.py` gives every script one env-overridable
  source for DB/Superset/paths. 28+ modules use it. The pattern is right (the committed
  default values are not; see F8).
- **Documented schema convention** — `schema/README.md` states the idempotent-SQL contract,
  the pipeline-owned marker, and where views live. Rare and valuable.
- **Idempotent, re-runnable SQL** everywhere (`CREATE TABLE IF NOT EXISTS`, guarded
  `ADD COLUMN`) — re-application is safe by construction.
- **Plans/results/as-built docs discipline** — `docs/<topic>_<plan|results>_<date>.md` is an
  excellent research-engineering habit and directly feeds the thesis. (Undermined only by
  the fact most of them are not in git; see F1.)
- **Operational robustness in the nightly** — `flock` locking, pre-ingest `pg_dump` backups
  with retention, Python-module preflight, dirty-source preflight that skips the seed step
  (the seed-overwrite protection), tee'd logs.
- **Deploy with parity checking** — `deploy_to_opt.sh` fast-forwards to a committed ref and
  refuses on dirty state; `scripts/ingestion_fingerprint.py` captures DB-state fingerprints
  to verify migrations. This is real rigor.
- **Test discipline on the new physics core** — the proxy/calibration/viewer modules ship
  with fast, pure-function unit tests (~239 tests), and new work consistently adds them.
- **Shared layers were extracted when pain appeared** — `common.py` (dedup of ingestion
  helpers) and `superset_api.py` (dashboard/chart API client) show the right instinct; the
  extraction just never happened for chart params/layout (F4).

---

## 3. Findings

Ordered roughly by (impact × urgency), not by theme.

### F1 — Thesis-critical documentation is not under version control (urgent)

`.gitignore` line 14 ignores `docs/` wholesale ("Local reference library"), with 30 files
force-added as exceptions. But **19 top-level docs are untracked**, including:

- `docs/thesis_outline_draft_2026-07-06.md` (the thesis outline itself),
- every mechanistic-energy-proxy plan/result/handoff since 2026-06-23 (the as-built record
  of the thesis's core contribution),
- `docs/proxy_truth_curation_runbook_2026-07-01.md`,
- `docs/proxy_severity_truth_mask_rollout_{plan,results}` (the latest major change).

These exist only as single copies on a CIFS NAS share (which has known transient-write
quirks). One accidental `rm`, an overwrite, or NAS trouble loses months of as-built
documentation that the thesis chapters are supposed to be written from.

Root cause: `docs/` mixes three different content types — 270 MB of paper PDFs
(`relevant_papers/`, correctly not in git), screenshots, and canonical project text docs —
so the ignore rule that was right for papers swallowed the documentation.

**Fix (minutes, do first):** invert the ignore — replace `docs/` with
`docs/relevant_papers/` (and optionally `docs/readiness_screenshots/`), then
`git add docs/*.md docs/n_phase_plan/`. All future docs get tracked by default.

### F2 — `data_processing_scripts/` is a god-folder; no real package structure

- 52 modules spanning ingestion, seeding, schema application, physics, calibration, ML
  training, Superset dashboard building, HTML/JS code generation, CSV exports, plotting,
  an API client, one-time scripts, and dead legacy code — one flat namespace.
- There is an empty `__init__.py` (so it *is* importable as a package), but the modules
  import each other flat (`from db_config import …`), which only resolves when the CWD or
  `PYTHONPATH` is the folder itself. Consequences visible everywhere:
  - the nightly must `cd` into the folder and export
    `PYTHONPATH="${INGEST_DIR}:${REPO_ROOT}"` (`scripts/nightly_update_and_ingest.sh:200,227`);
  - 7 files carry `sys.path.insert(0, …)` hacks;
  - 5+ modules carry try/except dual-import shims
    (`calibrate_mechanistic_energy_proxy.py:41-43`, `radiation_stress_dose.py:21-29`, …);
  - tests mix package-style imports with their own path hacks
    (`tests/test_seed_irradiation_campaigns.py:7`);
  - `server.py:90` imports the same module a third way
    (`from data_processing_scripts.db_config import …`).
- There is no `pyproject.toml`, no `conftest.py`, no `pytest.ini` — the repo is not
  installable, and how to run anything (which venv, from which directory) is tribal
  knowledge (`pytest` isn't even importable from the system Python on this machine).

### F3 — Dead code and root-level cruft

- `DatabaseScript.py` (998 lines) is imported/executed by nothing; it is the 2022-era
  ingestion superseded by `ingestion_*.py`. It is the only consumer of `datasheetgm.py`
  (883), `datasheetrdson.py` (916), `datasheetvth.py` (60) — a ~2,860-line dead island
  (~14 % of the folder). (A second, older copy of `DatabaseScript.py` also sits one level
  above the repo in the NAS share.)
- Root cruft: `.gitignore.save` (tracked!), `default` (tracked nginx config that has since
  diverged from the live `server_config/nginx_config` — two "truths" for the same server),
  `__pycache__/` and `.pytest_cache/` sitting in the repo root.
- `out/` is gitignored *except* 8 grandfathered files including three `.joblib` pickled
  models and PNGs — binary artifacts in git history that no longer match the code that
  produces them.
- A stale linked worktree `.claude/worktrees/mech-energy-phase4` (branch
  `worktree-mech-energy-phase4`, commit `dc582c1`) duplicates the whole tree inside the
  repo; another detached worktree lingers at `/tmp/aps-deploy.zUOien`.

### F4 — Nine dashboard builders re-implement the same framework (~10 kLOC)

Each `create_*_dashboard.py` re-defines its own `build_dashboard_layout`,
`build_native_filters`, `cat_filter`, `source_rows_only_filter`, `waveform_params`,
`scatter_params`, `table_params`, `sql_filter`, … with small drifts. `superset_api.py`
correctly centralizes the HTTP client, but the *chart-param / native-filter / tabbed-layout
DSL* — the majority of those 10 kLOC — is copy-pasted. Every cross-cutting change (e.g. the
device filter added in June) means editing up to nine files, and drift between copies is
already visible (some builders have `apply_schema`/`ensure_views_exist`, others don't).

Also: `superset_api.py:17` imports from `dashboard_png_export` — the generic API client
depends on a specialized exporter (inverted dependency; the export helpers belong in the
client or a third module both can use).

### F5 — Single-file monoliths mixing unrelated stages

- `ml_post_iv_physical_prediction.py`: 3,343 lines, 93 functions, 19 CLI flags — SQL DDL
  rebuild, feature extraction, pair building, training, validation, and curve prediction in
  one file. `ml_sc_irrad_equivalence.py`: 2,234 lines.
- `create_interactive_damage_signature_viewer.py`: 3,064 lines — SQL, data prep, physics
  post-processing, *and* a ~500-line HTML/CSS/JS application embedded as a Python string
  (`HTML_TEMPLATE` at line 2540, JS runtime injected at 2532). The 9-tab viewer JS is
  edited without syntax highlighting, linting, or diffable structure; every UI tweak is a
  Python-string edit in a 122 KB file.
- `server.py`: 935 lines, 24 routes, two unrelated applications (2022 file-search + 2026
  CRUD admin for devices/campaigns/runs) with inline SQL in every handler.

### F6 — SQL lives in three places; live-DB state is unknowable from the repo

- Schema DDL: `schema/*.sql` (numbered 000, 001, 020–028 with gaps, **plus** two unnumbered
  files `device_mapping_rules.sql`, `irradiation_campaign_dedup.sql` that sort into the
  middle of the sequence).
- Views: some in `schema/`, some as Python heredocs inside owning scripts
  (`baselines_view` in `ingestion_baselines.py`, `ensure_view_exists()` in dashboard
  builders) — per the documented convention, but it means no single place shows the
  database's shape.
- Application: `common.apply_schema()` re-applies every core file at every ingestion start;
  "pipeline-owned" files are gated by a magic comment in the first 500 bytes.
- There is **no migration ledger** — nothing records which SQL has been applied to which
  database. This is not hypothetical: the project history includes exactly this failure
  mode (schema 025 believed applied but wasn't; live/builder dashboard drift with 35 orphan
  charts). Idempotency makes re-applying safe, but "just re-apply and hope" is the only
  diagnostic available.

### F7 — Orchestration is a 273-line bash program

`scripts/nightly_update_and_ingest.sh` hard-codes `/opt` paths and encodes the entire DAG
as an ordered list of 20+ `run_py` calls, with inline `python -c` SQL for a materialized-view
refresh (line 243) and a guard for a script that doesn't exist in the repo
(`create_baselines_dashboard_device_library.py`, lines 246–250 — an admission that deploy
and repo have already drifted). Bash gives you: no `--only`/`--from`/`--skip` for reruns,
no per-step timing/skip logic, no shared logging with the Python code, and the step list
can't be unit-tested. The robustness features (locking, backups) are good — they just
deserve a better host.

### F8 — Secrets and environment values committed as defaults

- `db_config.py`: production DB password (`APSLab`) and Superset admin credentials
  (`admin`/`admin`) as committed defaults; `DATA_ROOT` defaults to a personal home
  directory path.
- `superset/docker-compose.yml:90`: `POSTGRES_PASSWORD: "APSLab"`.
- `superset/superset_config.py:46`: `SECRET_KEY = "TEST_NON_DEV_SECRET"` (signs Superset
  session cookies in prod).
- `server.py:38`: Flask fallback `SECRET_KEY 'vRbPDgZP6rHpjCSQWByy'`.

Mitigated by nginx basic-auth on an internal lab server, but the values are in git history
forever and match production. There is also no `.env.example`, so the set of supported
`APS_*` variables is only discoverable by reading source.

### F9 — One `requirements.txt` for three different environments, half stale

The file is a 2022 `pip freeze` of the Flask venv (pinned Flask 2.2.2 era, including dev
tools `ipython`/`jedi` and the **obsolete `pathlib==1.0.1` backport**, which can shadow the
stdlib and break modern tooling) with unpinned 2026 pipeline/ML additions appended. Server,
pipeline, and dev dependencies are indistinguishable; nothing is lock-filed; the nightly
compensates with a hand-rolled import preflight for 11 modules.

### F10 — Tests cover only the newest layer; no CI, no lint/format config

- ~239 fast unit tests, but all concentrated on the 2026 proxy/physics/viewer core.
  Ingestion (9.3 kLOC), dashboard builders (10.2 kLOC), ML (5.6 kLOC), and `server.py`
  have **zero** tests.
- No `conftest.py`/`pytest.ini` (import behavior depends on invocation directory), no CI of
  any kind, no ruff/black/mypy/pre-commit. Style drift is visible (tabs in `server.py`,
  spaces elsewhere; `DatabaseScript.py` vs `snake_case` names).
- The nightly runs the pipeline against production without running the test suite first.

### F11 — The repo lives inside the data share

The working copy sits in the NAS data tree next to `Measurements/`, student folders, and
loose PDFs. CIFS is a known source of transient write anomalies (already bitten this
project), git operations on CIFS are slow and lock-fragile, and `docs/relevant_papers/`
(270 MB) + `out/` (194 MB) sit inside the repo directory. Code and data have different
backup/versioning needs; co-locating them serves neither.

---

## 4. If this codebase were written from scratch

The system it needs to be: **a PostgreSQL research database + a nightly batch ETL/analysis
pipeline + three presentation surfaces (Superset dashboards, static interactive HTML tools,
a small Flask CRUD/search app), operated by 1–2 people on one server.** The right shape for
that is a single installable Python package with layered subpackages, one CLI, one config
story, SQL in one place, and thin deploy/orchestration shims — not microservices, not
Airflow, not a framework. Concretely:

```
aps-database/
├── pyproject.toml               # one package `aps`, console-script `aps`, extras:
│                                #   [server], [ml], [dev]; lockfile via uv or pip-tools
├── README.md                    # what the system is NOW: db + pipeline + dashboards + thesis
├── .env.example                 # every APS_* variable documented; real values never committed
├── .gitignore                   # data/, out/, docs/papers/ — nothing else surprising
│
├── src/aps/
│   ├── config.py                # today's db_config.py, grown up: one Settings object,
│   │                            #   reads env/.env, no production secrets as defaults
│   ├── db/
│   │   ├── connect.py           # get_connection(), cursor helpers
│   │   ├── migrate.py           # applies migrations/ in order, records each in a
│   │   │                        #   schema_migrations ledger table (file, checksum, when)
│   │   └── migrations/          # ALL DDL: numbered NNN_*.sql, no gaps, no unnumbered files,
│   │                            #   views as .sql too — the repo shows the database's shape
│   ├── ingest/                  # file → rows. one module per source family:
│   │   ├── baselines.py, short_circuit.py, avalanche.py, irradiation.py, logbooks.py
│   │   ├── matching.py          # device matching (from common.py)
│   │   └── categorize.py        # measurement categorization (from common.py)
│   ├── seeds/                   # seed_device_library, seed_campaigns, seed_truth_labels…
│   ├── enrich/                  # extract_damage_metrics, extract_single_event_effects,
│   │                            #   radiation_stress_dose, irradiation_energy_windows
│   ├── proxy/                   # the thesis core: mechanistic_energy_proxy,
│   │   │                        #   depletion_threshold_model, distance calibration,
│   │   ├── calibrate.py         #   truth-label metrics (pure functions, as today)
│   │   └── apply.py
│   ├── ml/
│   │   ├── post_iv/             # ml_post_iv split by stage:
│   │   │   ├── features.py, pairs.py, train.py, validate.py, predict.py
│   │   └── sc_equivalence/      # same treatment
│   ├── superset/
│   │   ├── client.py            # today's superset_api.py (without the png-export import)
│   │   ├── dsl.py               # THE shared layer that never got extracted: chart params,
│   │   │                        #   native filters, tabbed layout builder, slug/naming
│   │   ├── export_png.py
│   │   └── dashboards/          # one DECLARATIVE spec per dashboard (~150–300 lines each):
│   │       ├── baselines.py     #   datasets + chart specs + tab layout, no framework code
│   │       ├── irradiation.py, sc.py, avalanche.py, proxy_readiness.py, …
│   ├── viewers/                 # interactive HTML tools
│   │   ├── damage_signature/
│   │   │   ├── data.py          # SQL + payload prep (pure, testable — as today)
│   │   │   ├── template.html.j2 # real HTML file, editable/lintable
│   │   │   └── viewer.js        # real JS file; build step inlines it into the template
│   │   └── sc_equivalence/…
│   ├── exports/                 # CSV export commands
│   └── cli.py                   # `aps migrate`, `aps ingest baselines`, `aps seed …`,
│                                # `aps dashboards build proxy-readiness`, `aps nightly`,
│                                # `aps nightly --from extract --skip ml`, `aps fingerprint`
│
├── src/aps_server/              # the Flask app as an app-factory package
│   ├── app.py                   # create_app(); config from aps.config
│   ├── blueprints/              # search.py (2022 feature), devices.py, irradiation.py,
│   │   │                        #   avalanche.py — today's 935-line server.py split
│   ├── repo.py                  # the inline SQL, gathered into query functions
│   └── templates/, static/
│
├── pipelines/
│   └── nightly.py               # the DAG as data: ordered [Step(name, fn, gate)] list,
│                                #   per-step timing/logging/failure record; bash shrinks to
│                                #   a 15-line systemd wrapper (venv + `aps nightly`)
├── deploy/
│   ├── systemd/                 # aps-nightly.service/.timer, server.service (as today)
│   ├── nginx/aps.conf           # ONE nginx truth (today's root `default` deleted)
│   ├── superset/                # docker-compose.yml + superset_config.py
│   └── deploy_to_opt.sh         # today's script, pointed at the new layout
│
├── tests/                       # mirrors src/aps; conftest.py at repo root ends all
│   │                            #   sys.path hacks; markers: unit (default, no DB) and
│   │                            #   integration (needs a scratch Postgres, opt-in)
├── docs/                        # TRACKED by default
│   ├── index.md                 # one-line map of every doc (like MEMORY.md)
│   ├── plans/, results/, runbooks/, thesis/
│   └── papers/                  # the only ignored subtree (270 MB of PDFs stay off git)
└── archive/                     # or simply deleted — git history remembers
    ├── DatabaseScript.py, datasheet*.py, one_time_scripts/
```

Key decisions and why:

1. **Installable package + console entry points** kills the entire class of import problems
   (F2): no `cd` coupling, no `PYTHONPATH` exports, no `sys.path.insert`, no dual-import
   shims, tests import the same way production runs.
2. **Layering follows the data flow** — `ingest → seeds/enrich → proxy/ml → superset/viewers/exports` —
   and only `db/migrations` may define schema. Today, dashboard builders and ML scripts
   `ALTER`/`CREATE` on the fly; from scratch, "what shape is the database" has one answer.
3. **A migration ledger table** (hand-rolled, ~40 lines; Alembic is optional, not required)
   keeps the idempotent-SQL style but records what ran where — directly addressing the
   "was 025 applied live?" incidents (F6).
4. **Dashboards become declarative specs over a shared DSL** (F4): the nine builders carry
   ~10 kLOC today; specs over a common `dsl.py` would be roughly a third of that, and a
   cross-cutting change (new filter, new tab style) becomes a one-file edit.
5. **Viewers separate data from presentation** (F5): Python computes the JSON payload;
   HTML/JS are real files with real tooling. A build step inlines assets for the
   self-contained single-file deployment nginx serves today.
6. **Orchestration in Python, scheduling in systemd** (F7): the DAG is a testable step list
   with `--only/--from/--skip` for the "re-run one stage" workflow you already do manually;
   systemd timer and the excellent backup/locking behavior are retained (moved into steps).
7. **One config story** (F8/F9): `aps.config.Settings` + `.env` (+ committed
   `.env.example`); prod secrets fail-fast if unset rather than defaulting to real values;
   dependency extras separate server/pipeline/ml/dev; a lockfile replaces the frozen-2022 +
   unpinned-2026 hybrid.
8. **Repo outside the data share** (F11): the working copy lives on local disk (or a
   GitLab/GitHub remote as primary), with `APS_DATA_ROOT`/`APS_NAS_ROOT` pointing at the
   share — the env-var plumbing for this already exists.
9. **CI-lite**: ruff + pytest on every push (GitHub Actions or a pre-push hook — even just
   pre-commit locally). One person plus an agent still benefits: it's the drift detector.

What I would *not* do from scratch: microservices, an ORM rewrite of the raw-psycopg2 style
(it's consistent and fine for this scale), Airflow/Dagster (one nightly on one box), or a
JS build ecosystem for the viewers beyond a trivial inline step.

---

## 5. Mapping: current file → target home

| Today | Target |
|---|---|
| `data_processing_scripts/db_config.py` | `src/aps/config.py` |
| `data_processing_scripts/common.py` | split: `aps/db/migrate.py` (apply_schema) + `aps/ingest/matching.py` + `aps/ingest/categorize.py` + file utils |
| `ingestion_*.py`, `parse_logbooks_assign_runs.py`, `promote_to_baselines.py` | `aps/ingest/` |
| `seed_*.py` | `aps/seeds/` |
| `extract_*.py`, `radiation_stress_dose.py`, `irradiation_energy_windows.py` | `aps/enrich/` |
| `mechanistic_energy_proxy.py`, `calibrate_*.py`, `apply_mechanistic_energy_proxy.py`, `depletion_threshold_model.py` | `aps/proxy/` |
| `ml_post_iv_physical_prediction.py`, `ml_sc_irrad_equivalence.py` | `aps/ml/post_iv/`, `aps/ml/sc_equivalence/` (split by stage) |
| `superset_api.py`, `dashboard_png_export.py` | `aps/superset/client.py`, `aps/superset/export_png.py` |
| `create_*_dashboard.py` (9 files) | `aps/superset/dsl.py` + `aps/superset/dashboards/<name>.py` |
| `create_interactive_*_viewer.py`, `plot_*.py` | `aps/viewers/<tool>/` (data.py + template + js), `aps/viewers/plots/` |
| `export_proxy_*.py` | `aps/exports/` |
| `server.py`, `data_scraping.py`, `templates/`, `static/`, `wsgi.py` | `src/aps_server/` package (blueprints), `wsgi.py` kept as shim |
| `schema/*.sql` | `src/aps/db/migrations/` (renumbered contiguously; views included) |
| `scripts/nightly_update_and_ingest.sh` | `pipelines/nightly.py` + 15-line systemd wrapper |
| `scripts/deploy_to_opt.sh`, `scripts/aps-nightly-failure-record.sh` | `deploy/` |
| `scripts/ingestion_fingerprint.py` | `aps/cli.py fingerprint` subcommand |
| `server_config/*`, `superset/*` | `deploy/` (nginx/systemd/superset) |
| `config.ini`, `server.ini` | `deploy/` (uwsgi) / folded into `aps.config` |
| root `default`, `.gitignore.save`, `DatabaseScript.py`, `datasheet*.py`, `one_time_scripts/` | delete (git history preserves) or `archive/` |
| `out/` tracked binaries (`*.joblib`, PNGs) | untrack; `out/` fully ignored |
| `docs/` | tracked by default; only `docs/papers/` (and optionally screenshots) ignored |

---

## 6. Pragmatic adoption path (thesis-aware; original 2026-07-06 plan)

> Historical plan retained to explain the commits and decisions above. It is not the
> current remaining-work list. The independent follow-up found additional release,
> database-lifecycle, reproducibility, and operational work; §7 supersedes this section
> for execution from 2026-07-11 onward.

The repo **is** the thesis evidence, the nightly runs against production, and
`deploy_to_opt.sh` assumes current paths. A big-bang restructure before the thesis snapshot
would put the deadline at risk for zero scientific gain. Phased, additive, in priority
order:

**Phase 0 — safety and hygiene (≈1 hour, zero behavior change, do now):**
1. Fix `.gitignore`: `docs/` → `docs/relevant_papers/`; `git add` all untracked `docs/*.md`
   (incl. the thesis outline) and commit. *This is the single highest-value change in this
   review.*
2. Consider pushing to a remote (ETH GitLab / GitHub private) if not already — the NAS is
   currently the only copy of the git history too.
3. Delete `.gitignore.save`; delete or move root `default` after confirming
   `server_config/nginx_config` is the deployed truth.
4. `git worktree remove .claude/worktrees/mech-energy-phase4` (after confirming its branch
   is merged) and `git worktree prune` the `/tmp` leftover.
5. Untrack the stale `out/` binaries (`git rm --cached out/...`.joblib etc.).

**Phase 1 — packaging shim (≈half a day, low risk, big daily payoff):**
1. Add `pyproject.toml` declaring the existing `data_processing_scripts` as a package
   (no renames yet) + split requirements into extras; remove `pathlib==1.0.1`, `ipython`,
   `jedi` from runtime deps.
2. Add root `conftest.py` (two `sys.path` lines) and `pytest.ini`; delete the per-file
   path hacks; standardize on package-style imports as files get touched.
3. Add `.env.example`; move real passwords out of committed defaults (env-only on the
   server), rotate when convenient.
4. Optional: `pre-commit` with ruff (lint only, no reformat churn before the freeze).

**Phase 2 — after the thesis snapshot/freeze tag:**
1. Delete the dead legacy island; `git mv` into the target subpackage layout (history
   follows).
2. Extract the Superset DSL from the two most-maintained builders (proxy-readiness,
   irradiation) and convert the rest opportunistically.
3. Extract viewer HTML/JS into template + asset files with an inline build step.
4. Replace the bash step list with `pipelines/nightly.py` (keep systemd + backups); add the
   `schema_migrations` ledger; renumber schema files.
5. Split `server.py` into blueprints; split the ML monoliths along their existing CLI-flag
   seams.

Suggested commit checkpoint: tag the pre-restructure state (e.g. `pre-reorg-2026-07`) so the
thesis can cite a stable tree while Phase 2 proceeds.

---

## 7. Completion plan — 2026-07-11

### Planning principles

The order below is based on failure radius, not visual neatness. Production/data safety,
reproducibility, and a truthful release process come before splitting large files. The
directory move has already consumed most of the safe “mechanical reorganization” budget;
the remaining work changes ownership and runtime behavior and therefore needs smaller,
verified commits.

Four constraints apply throughout:

1. **Do not deploy from the current dirty working tree.** Preserve the work in intentional,
   reviewable commits first; ensure every referenced untracked schema/module/test is in the
   same commit series.
2. **Do not restore the removed legacy ingestion script.** It drops all public tables.
   Recover only its required parsing/lineage behavior behind a new safe importer.
3. **Do not combine a PostgreSQL/Superset/Redis image upgrade with a code/schema release.**
   Those need separate backup, verification, and rollback windows.
4. **Do not delete legacy tables or old materialized models during Phases 0–2.** Build
   replacements beside them, compare, cut consumers over, and schedule deletion only after
   a retained backup and an explicit owner decision.

Rough effort ranges below assume one engineer on this deploy server and are deliberately
not calendar promises. The acceptance criteria matter more than the estimate.

### Phase 0 completion — stabilize and reconcile reality

**Goal:** establish one recoverable release baseline before more architecture work.
Expected effort: roughly 1–3 focused days plus a coordinated credential/service window.

#### 0.1 Preserve and classify the in-progress work

- Create a named branch/checkpoint for the July 10–11 dashboard work.
- Split it into atomic commits: scientific SQL/proxy changes; dashboard presentation
  changes; portfolio reconciliation/backfill; dynamic CV/DPT work; tests; nightly wiring.
- Keep `schema/030_dynamic_characterization.sql` and its nightly invocation out of the
  release candidate until §0.3's ownership decision is satisfied.
- Inspect the locked viewer worktree, port or explicitly discard its one owner edit, then
  remove/prune the worktree only after that decision is recorded.
- Re-run the default offline unit suite after §1.3 separates the live-DB tests; until then,
  record that “280 passed” used production data.

**Acceptance:** no feature is referenced by a tracked script while its implementation is
untracked; the branch is clean; each commit has a stated rollback boundary; the locked
worktree has a documented disposition.

#### 0.2 Capture a release and recovery inventory

- Record source SHA, `/opt` SHA, installed unit paths/content, running process command,
  container image IDs/digests, PostgreSQL/Superset versions, active timers, database size,
  and artifact publication roots.
- Take both data and Superset-metadata dumps using the already-established backup process.
- Restore the dumps into disposable containers/databases and run a minimal catalog/row
  fingerprint. A backup that has never been restored is not yet a verified rollback.
- Export a schema-only catalog and an inventory of the 735 public tables, identifying
  current canonical tables, legacy per-measurement tables, aggregates, ML outputs,
  materialized views, and unknown/orphan objects.

**Acceptance:** a dated recovery record names the exact code/database/container state and
contains a successfully tested restore procedure; no live object is classified only from
memory.

#### 0.3 Quarantine and decide ownership of legacy CV/DPT data

Recommended choice: build a new canonical importer, because a dashboard presented as a
current pipeline surface should be reproducible.

- Immediately classify 030 as an explicitly owned derived model, not an unconditional core
  startup migration. Add a fail-fast prerequisite check for `cpvd`, `dptgraphs`, and
  `dptslopes` if the temporary legacy-backed version is run.
- Freeze and fingerprint those three aggregate tables plus their relevant source files.
- Write an as-built note stating that their current producer was deleted and the live
  tables are historical state.
- Choose and record one of M3's two paths. If “snapshot only” is chosen, remove the nightly
  builder call and label the dashboard/data date. If “canonical importer” is chosen, §1.5
  becomes a release gate.

**Acceptance:** a fresh database cannot accidentally try to apply 030 without its inputs;
the dashboard cannot imply nightly freshness when no ingestion owner exists.

#### 0.4 Finish secrets and environment provisioning

- Provision root/service-readable environment files outside git (for example,
  `/etc/aps/aps.env` and a separate Superset Compose env file) with least-privilege file
  modes.
- Make `server.service`, the nightly unit, Compose, manual CLI invocation, and local
  development use one documented variable contract.
- Replace production-like password/secret defaults with required values or explicitly safe
  development-only values. Refuse to start a production profile with placeholders.
- Rotate PostgreSQL, Superset admin, Flask signing, and Superset signing secrets after the
  consumers are wired. Rotation and code deployment must be coordinated to avoid an
  outage.
- Bind PostgreSQL to loopback unless remote database access is explicitly required and
  protected by a documented firewall/access rule.
- Replace remaining personal hard-coded baselines/SC/data paths with validated settings.

**Acceptance:** repository search finds no production credential; production startup fails
clearly when a required secret is absent; all services load the same documented config;
the old credentials no longer authenticate.

#### 0.5 Stop runtime environment mutation

- Remove all import-time/on-demand `pip install` branches.
- Add every actual runtime dependency to the appropriate package extra and add uWSGI (or
  explicitly document/system-package it) for the server environment.
- Generate a reviewed lock/constraints file for the deploy environment, including hashes
  where the chosen tool supports them.
- Build a fresh disposable venv from that lock, install the package, import every module
  from a foreign CWD, and run the offline suite.

**Acceptance:** application code never invokes pip; the deploy can be built without
reusing `~/aps_venv`; missing dependencies fail in preflight with an actionable error.

#### 0.6 Make release/deploy state explicit

- Correct the repository's server unit to the intended `/opt` checkout and compare the
  full installed unit with the tracked version.
- Extend deployment into explicit stages: validate clean committed ref; backup; build or
  install the locked environment; apply forward migrations; build required derived models;
  restart the affected service; run HTTP/DB/Superset smoke checks; emit a release manifest.
- Keep the previous code SHA and environment available for code rollback. Document that a
  database restore/forward fix may still be required after a non-backward-compatible
  migration.
- Install/enable the nightly service and timer only after a manual dry/shadow run succeeds
  and its environment file, mount requirements, timeouts, and failure handler are verified.
- Move `docker compose pull/up` to a separate maintenance command. Normal nightly ETL
  should check service health, not upgrade infrastructure.

**Acceptance:** active service SHA equals the declared release SHA; tracked and installed
units match; a timer status and next-run time are visible; smoke checks fail the deploy on
stale/broken components; rollback steps have been rehearsed.

### Phase 1 completion — make package, database, and tests reproducible

**Goal:** turn the Phase 1/ledger shims into enforceable contracts that work from a clean
machine and a blank disposable database. Expected effort: roughly 4–8 focused days.

#### 1.1 Introduce one settings boundary and one CLI

- Replace module-level `db_config.py` globals with a validated settings object/factory.
  Keep a temporary compatibility module while callers migrate.
- Support explicit profiles such as `development`, `test`, and `production`; never
  infer production merely because localhost:5435 is reachable.
- Resolve paths once, validate source/read/write requirements, and pass settings into
  application/pipeline entry points instead of importing mutable globals.
- Add console commands such as `aps db status/migrate`, `aps models build`,
  `aps ingest ...`, `aps dashboards ...`, `aps nightly ...`, and
  `aps release status`. Module entry points can remain as compatibility shims initially.
- Remove `PYTHONPATH` and `sys.path.insert` only after both the Flask app and scripts are
  installed entry points.

**Acceptance:** commands run from any CWD in a clean installed venv; test configuration
cannot silently select the production database; all supported variables appear in one
generated/documented reference.

#### 1.2 Split forward migrations from repeatable derived models

Proposed layout (names may remain 020–030 for thesis traceability):

```
sql/
├── migrations/    # append-only structural/data migrations; applied once
├── models/        # repeatable view/materialized-view definitions + dependency metadata
├── seeds/         # controlled reference data
└── checks/        # invariants, row-count/uniqueness/freshness assertions
```

- Make migrations immutable once applied. A checksum mismatch must stop with an explicit
  diagnostic; a change requires a new migration file.
- Add a database advisory lock and a uniqueness constraint so concurrent migrators cannot
  race or duplicate ledger entries.
- Record start/end/status/error as well as checksum; do not mark a failed transaction
  applied.
- Move direct and embedded DDL into owned SQL assets incrementally, starting with the
  1,750-line SC-equivalence bundle, baselines schema, seed-campaign schema, and
  025/028/029 call paths.
- Define each derived model's owner, inputs, outputs, checksum, build mode
  (`view`, `refresh`, `replace`, or staged swap), expected indexes, and validation
  checks.
- Add a model-build ledger with code SHA, input/upstream versions, timing, row count,
  status, and error. Do not pretend this is the forward-migration ledger.
- Remove all schema application from Flask import and all derived-model rebuilding from
  dashboard creation.

**Acceptance:** a scratch PostgreSQL instance reaches the supported schema from zero using
one command; rerunning migrations is a no-op; editing an applied migration fails closed;
every supported live object maps to either a migration or an owned model asset; a model
build is auditable independently from migration state.

#### 1.3 Establish honest test tiers and CI

- Mark the fast, infrastructure-free suite `unit` and make it the default.
- Move `test_stress_context_figure1b.py` to an opt-in `production_smoke` tier or rewrite
  its logical assertions over fixtures. It must require an explicit production-smoke
  opt-in and read-only credentials.
- Add `integration` tests against disposable PostgreSQL for migrations, model dependency
  ordering, idempotency, constraints, transaction rollback, and representative ingestion.
- Add Flask app-factory route/CSRF/repository tests with temporary configuration.
- Add Superset HTTP contract tests using a local fake server or strict session double,
  including timeout/non-2xx/partial-update behavior.
- Install and run Ruff in CI; initially baseline/exclude known legacy style debt and ratchet
  only changed code rather than formatting 42 kLOC at once.
- Add a lightweight architecture test for forbidden imports, especially
  `superset -> ingest`, API client -> exporter/presentation policy, and future
  application -> migration internals.

**Acceptance:** `pytest` passes offline with no DB/network; integration and production
smoke commands are separate and clearly named; CI runs unit + lint on every push and
integration tests on the chosen protected workflow; no test can reach production by
default.

#### 1.4 Define and enforce data contracts

- Document source-of-truth tables, derived tables, views, and ownership for each flow:
  baselines, SC, irradiation, avalanche, CV/DPT, proxy, and ML.
- Treat the 77-column `baselines_metadata` table as an intentional compatibility hub for
  now; do not attempt a big-bang normalization during the thesis work. New source-specific
  fields should go into extension tables keyed by metadata id unless there is a justified
  cross-source contract.
- Move shared SQL/constants imported across layers into neutral domain/model modules
  (for example the avalanche view definition and damage extraction query).
- State keys, units, null semantics, provenance fields, destructive/rebuild behavior, and
  freshness expectations for each contract.

**Acceptance:** every pipeline step declares inputs/outputs and units; no presentation
module imports an ingestion implementation to acquire schema; new source-specific columns
do not continue widening the shared table without review.

#### 1.5 Rebuild CV/DPT provenance safely

If §0.3 selected the recommended canonical path:

- Create canonical `cv_measurements`, `dpt_captures`,
  `dpt_waveform_points`, and `dpt_switching_metrics` (exact names can vary) with source
  path/hash/mtime, device/sample, units, parser version, and ingestion timestamp.
- Recover the required parsers from git history but rewrite execution as incremental,
  idempotent, source-scoped ingestion. It must never enumerate/drop unrelated tables.
- Import into the canonical tables beside the frozen legacy model.
- Compare row counts, capture counts, min/median/max ranges, units, and sampled waveforms;
  explain rather than silently accept discrepancies.
- Point 030's views at canonical tables, add scratch-DB fixtures, and only then enable its
  dashboard/nightly step.

**Acceptance:** a clean database can reproduce the CV/DPT dashboard from source fixtures
and then from the real corpus; parity is recorded; legacy tables remain read-only until a
later explicit retirement plan.

### Phase 2 completion — orchestration, performance, and maintainable surfaces

**Goal:** finish the responsibility boundaries promised by the package layout without
changing scientific results silently. Expected effort: roughly 2–4 incremental weeks,
shipped as independent slices.

#### 2.1 Replace the bash DAG with a Python run manifest

- Keep a thin systemd/shell wrapper for the OS-level lock, environment, initial backup,
  and invocation. Express data steps as named Python `Step` records with dependencies,
  required capabilities, critical/optional policy, and callable/command.
- Support `--plan`, `--only`, `--from`, `--until`, and `--skip`, with dependency
  validation so an unsafe partial run is rejected.
- Create `pipeline_runs` and `pipeline_step_runs` records containing release SHA,
  configuration fingerprint (excluding secrets), source fingerprint, start/end/duration,
  status, row counts, model build ids, artifacts, and error.
- Define `success`, `degraded`, and `failed` explicitly. Optional viewer/export
  failures should produce a degraded run and alert, not disappear behind `|| true`.
- Make retry policy explicit. Retry only operations proven idempotent; never blindly retry
  a partial destructive/rebuild stage.

**Acceptance:** the DAG and partial-run validation are unit-tested; an interrupted run is
diagnosable and safely resumable; operators can identify the exact failed step and code
version without reading an entire text log.

#### 2.2 Build derived models once and control materialization cost

- Collapse all 025/028/029 execution into one dependency-aware model-build stage after
  upstream ingestion/enrichment. Remove rebuild calls from single-event extraction,
  dashboard creation, and the duplicate mechanistic apply step.
- Benchmark every materialized model: build time, rows, heap/TOAST/index size, lock time,
  consumers, and freshness requirement.
- Redesign `stress_proxy_candidate_ranked_view` first. Evaluate storing only the consumer
  columns/top-N rows, separating wide evidence payloads, pre-aggregating repeated inputs,
  and using staged tables or concurrent refresh where PostgreSQL constraints permit.
- Add disk-space and expected-row-count preflight, post-build `ANALYZE`, and invariant
  checks. Retain the previous usable model until the replacement validates when practical.
- Define a performance budget and alert on significant build-time/row-count/size drift.

**Acceptance:** 025 and 028 execute at most once per normal pipeline run; dashboard-only
rebuilds execute zero analytical DDL; model timing/size/rows are recorded; a failed new
build does not silently leave dashboards pointing at a partial model.

#### 2.3 Complete the Superset spec/client split

- Create a transport-only `SupersetClient` with configured connect/read timeouts,
  authentication validation, bounded retry only for safe requests, and exceptions/typed
  errors for all non-success responses.
- Move PNG export out of the client and make it a downstream consumer.
- Create shared typed/spec builders for metrics, filters, chart params, native-filter
  scoping, tab/row layout, descriptions, naming, and ownership.
- Convert two representative dashboards first (one simple, one Proxy Readiness), verify
  generated payload parity, then migrate the remainder.
- Use plan/apply/verify: compute the desired dashboard inventory; apply changes; fetch
  remote state; verify charts, associations, filters, descriptions, publication, and
  orphans; emit a reconciliation report. Preserve the last good dashboard on failure.
- Make every dashboard builder consume prepared database models; `--schema-only` belongs
  to the model CLI, not the presentation CLI.

**Acceptance:** simulated API failures produce nonzero exits; no association/update failure
is log-only; cross-cutting chart/filter changes occur in shared code/specs; reconciliation
finds no unexplained hidden attachments or orphans after a build.

#### 2.4 Split viewers along data/presentation boundaries

- Extract SQL/loading, scientific payload construction, and presentation serialization.
- Move HTML, CSS, and JavaScript into real template/assets files; retain a deterministic
  build step that emits the current self-contained HTML artifact.
- Add JavaScript syntax/lint checking, payload-schema tests, golden/snapshot structure
  tests, and a small browser smoke test for tab/filter/render startup.
- Record artifact SHA, build/run id, source model version, generated timestamp, and
  publication result.

**Acceptance:** scientific payload tests run without rendering; JS can be checked without
parsing a Python string; identical inputs produce a deterministic artifact apart from
explicit metadata; publish failure marks the run degraded.

#### 2.5 Convert Flask to an app-factory package

- Create `aps_server.create_app(settings, repositories, scanner)`.
- Split search, devices/mapping, irradiation, and avalanche into blueprints.
- Move inline SQL into repository/query functions with transaction tests.
- Remove filesystem scanning and schema application from import. Warm/refresh the file
  index explicitly and expose its age/error state.
- Resolve templates/static/config relative to the installed package, not process CWD.
- Document that nginx Basic Auth is the authorization boundary; add a trusted-proxy/direct
  access policy so bypassing nginx cannot silently expose admin CRUD.

**Acceptance:** importing the WSGI module performs no DB/NAS mutation or scan; the app can
be instantiated in tests with fakes; startup health distinguishes app, DB, NAS index, and
schema/model readiness.

#### 2.6 Split ML modules by lifecycle stage

- Separate schema/model assets, feature extraction, pair construction, training,
  validation/gating, prediction, persistence, and CLI orchestration.
- Give model runs explicit immutable configuration/data/code fingerprints and artifact
  references; do not rely only on “latest max(id)” semantics.
- Preserve existing pure functions and add parity fixtures before moving each stage.
- Make the nightly invoke one high-level ML command while allowing safe stage-specific
  reruns through the orchestrator.

**Acceptance:** each stage has a narrow input/output contract and tests; a model prediction
can be traced to code/data/config; splitting changes no validated fixture results.

#### 2.7 Make current operation discoverable

- Rewrite `Readme.md` around the current system: architecture/data flow, installation,
  config profiles, CLI, test tiers, database/model lifecycle, dashboards/viewers, deploy,
  scheduler, backup/restore, and rollback.
- Add a concise operations runbook and an architecture decision log for accepted risks:
  ignored docs, retained 020–030 numbering, raw psycopg2, one-server monolith, and legacy
  table retention.
- Generate or maintain a command/owner matrix so a future maintainer does not infer the
  nightly order from shell source.

**Acceptance:** a new maintainer can create a dev environment, run offline tests, create a
scratch database, understand which commands mutate what, and identify the production
release without private oral context.

### Recommended commit/release sequence

Keep these independently reviewable; do not collapse them into one “finish Phase 2”
commit:

1. **Safety checkpoint:** preserve current feature work; quarantine 030; resolve worktree.
2. **Runtime foundation:** settings/env wiring, secret removal/rotation preparation,
   locked dependencies, remove runtime pip.
3. **Test boundary:** offline default suite, marked integration/smoke tests, CI + Ruff.
4. **Database lifecycle:** forward migration runner, repeatable model registry/build ledger,
   no Flask startup migration.
5. **Legacy CV/DPT:** canonical importer + parity; then enable 030/dashboard.
6. **Orchestration/performance:** run ledger, one model-build stage, eliminate repeated
   025/028/029 rebuilds, optimize the 8.6-GB model.
7. **Superset:** strict client, declarative specs, plan/apply/verify reconciliation.
8. **Presentation/application splits:** viewer assets, Flask factory/blueprints, ML stages.
9. **Release:** deploy rehearsal, install verified units/timer, production smoke, manifest,
   rollback rehearsal, updated README/runbook.

### Definition of “Phase 0–2 complete”

The work should not be called complete until all of the following are true:

| Area | Completion condition |
|---|---|
| Source/release | Clean committed release; active `/opt` SHA, environment, units, and manifest agree; rollback is documented and rehearsed. |
| Configuration/security | No committed production secrets or personal mandatory paths; production profile fails closed; credentials rotated; DB exposure intentional. |
| Dependencies | Locked, reproducible environment; no runtime pip; package/CLI/WSGI run without path hacks. |
| Database | Forward migrations are immutable/once-only; repeatable models are separately owned/ordered/recorded; fresh scratch build works; no supported object is live-only by accident. |
| Legacy data | CV/DPT is either reproducibly ingested or explicitly frozen/labeled; no destructive legacy script is operational. |
| Pipeline | Testable DAG, run/step ledger, explicit partial reruns and degraded status; infrastructure upgrades separate; large models built once. |
| Tests | Offline default suite, disposable-DB integration suite, explicit read-only production smoke; CI runs the declared gates. |
| Superset/viewers | API failures fail the run; remote state is verified; builders do no DDL; viewer source assets are testable and artifacts traceable. |
| Flask/ML | No import-time infrastructure work; app factory/blueprints/repositories exist; ML stages and provenance are explicit. |
| Documentation | Current README and operations/recovery runbook match the deployed architecture; accepted risks are recorded rather than repeatedly rediscovered. |

This definition intentionally does not require microservices, an ORM rewrite, Airflow,
full static typing, or immediate normalization of all legacy data. Those would expand
scope without addressing the concrete failure modes found here.

---

## 8. Implementation checkpoint — 2026-07-12

This checkpoint supersedes the implementation-state claims in the 2026-07-11
evidence snapshot. It records repository changes only: no production database
command, deployment, service installation/restart, credential rotation, backup,
restore, container update, or Superset mutation was performed while implementing
the changes below.

### Completed repository slices

#### Runtime/configuration safety

- Added validated `Settings` with explicit
  `development`/`test`/`production` profiles, DB/Superset/Flask
  secret use boundaries, Superset connect/read timeouts, and validated data/NAS
  roots. `db_config.py` remains a compatibility facade.
- Removed all application-side runtime pip installation. Missing waveform/ML
  dependencies now fail with actionable messages.
- Removed mandatory personal source paths from active ingestion/application
  code. Per-flow path overrides remain explicit environment variables.
- Removed committed Flask and Superset signing-key fallbacks. The APS database
  Compose password is required from the environment and PostgreSQL is published
  on loopback only.
- Added tracked placeholder-only contracts:
  `server_config/aps.env.example` and
  `superset/compose.env.example`. The owner-immutable root
  `.env.example` was not forcibly unlocked or rewritten.
- Corrected tracked systemd units to the `/opt` release checkout and the
  shared `/etc/aps/aps.env` contract.

#### Package, dependency, and test boundary

- Added the `aps` console entry point and command groups for configuration,
  migration/model inspection/builds, and nightly plan/run.
- Added a reviewed Python 3.12 constraints baseline and declared the deployed
  uWSGI version in the server environment. Runtime extras remain separated from
  ML/pipeline/dev dependencies.
- Made the default pytest command offline by deselecting
  `production_smoke`. Production smoke requires the explicit
  `APS_RUN_PRODUCTION_SMOKE=1` opt-in.
- Added CI for constrained installation, offline pytest, and a ratcheted Ruff
  scope. Added executable architecture tests for runtime pip, proxy SQL
  ownership, Superset-to-ingestion imports, and client/export-policy inversion.

#### Database/model lifecycle

- Added `aps.db.migrations`: numbered non-model discovery, immutable
  checksums, advisory locking, running/applied/failed/baselined state, and
  fail-closed handling of an existing un-ledgered database. This is deliberately
  separate from legacy idempotent `common.apply_schema`.
- Added `aps.db.models` and the `aps_model_builds` ledger with
  code/config fingerprints, status/error, timing, and catalog output statistics.
- Registered `proxy-analytics` as the one Python owner of 025/028/029 and
  `legacy-cv-dpt` as an explicit opt-in snapshot model with source-table
  prerequisites.
- Removed 025/028/029 rebuilds from single-event extraction and dashboard
  creation. The nightly model stage builds the bundle once; the legacy
  mechanistic command is now a compatibility build/validation wrapper.
- Removed dashboard SQL/model application, including the Avalanche ingestion
  import and the post-IV schema rebuild. Dashboards consume or verify prepared
  relations.

#### Orchestration and presentation boundaries

- Added the declarative `aps.pipelines.nightly` DAG with named dependencies,
  safe partial-selection validation, critical/optional policies, and
  `pipeline_runs`/`pipeline_step_runs` ledgers.
- Replaced the executable shell step list with one `aps nightly run`
  invocation. The shell retains OS locking, backup, service health, retention,
  and maintenance-only infrastructure updates.
- Optional viewer/export/publish failures now create a degraded recorded run and
  nonzero service outcome. Viewer publication is an atomic named pipeline step.
- Added a timeout-aware, fail-closed Superset client. Only GET receives a bounded
  transient retry; mutating calls never retry. JWT and API CSRF are required,
  and non-success responses raise typed errors.
- Removed local CSV/PNG export side effects and presentation-description policy
  from the API helper. Non-Proxy builders own descriptions through presentation
  support; dashboard association failures now propagate through the strict
  client.
- Quarantined 030 with prerequisite checks and nightly opt-in
  `APS_ENABLE_LEGACY_CV_DPT=1`. Its dashboard verifies model readiness
  and cannot imply an unconditional fresh-database path.

#### Flask and operations documentation

- Added a compatibility `server.create_app` boundary. Importing
  `server` now performs no schema application and no filesystem cache
  warmup; the factory requires `APS_FLASK_SECRET_KEY` and accepts a
  scanner test double. WSGI uses the factory.
- Rewrote `Readme.md` around the current architecture/config/CLI/test
  tiers/database-model lifecycle/nightly/deployment/rollback behavior.
- Added tracked operations and architecture-decision records under
  `server_config/`, because this review remains ignored by owner choice.

### Verification evidence

- `python -m pip check`: no broken requirements.
- Default offline suite: **315 collected, 6 production-smoke tests deselected,
  309 passed** in 4.20 s.
- Focused Superset/config contract suite: 16 passed.
- Architecture/Superset/CV-DPT boundary suite: 15 passed.
- Every installed APS module imported from `/tmp`: **70 modules,
  0 failures**.
- `git diff --check` and
  `bash -n scripts/nightly_update_and_ingest.sh`: clean.
- WSGI factory import with an explicitly injected test key: clean, with no DB
  bootstrap or scanner warmup.
- Offline `aps db plan`, model dry-run, and nightly plan: clean.
- Compose configuration validated with the placeholder-only template: clean.
- Local Ruff was not run because Ruff is still absent from the mandated
  `~/aps_venv`; CI installs and executes it. This is a verification gap,
  not a passed local gate.

The 1,339 pytest warnings are emitted by the pinned Flask 2.2/Werkzeug 2.2
routing implementation on Python 3.12. They do not fail the current suite but
should be addressed in a separate dependency-upgrade window rather than mixed
with this architecture release.

### Intentionally not completed in repository code

The implementation above closes the high-failure-radius foundations but does
not make the full Phase 0–2 definition true. These remain separate engineering
slices:

1. Build the canonical incremental CV/DPT importer and record parity against
   the frozen aggregates before enabling its nightly surface.
2. Move the remaining embedded/source-table DDL into owned forward migrations
   and prove zero-to-supported-schema, idempotency, checksum refusal, and
   rollback against disposable PostgreSQL.
3. Benchmark/redesign the 8.6-GB proxy ranking model, add disk/lock/row/size
   budgets, and implement staged replacement where practical.
4. Finish typed declarative Superset specs and full post-apply remote
   reconciliation across all builders. Existing portfolio reconciliation is
   dry-run-safe but is not yet a transactional desired-state engine.
5. Extract viewer HTML/CSS/JS into testable assets and add JS/browser smoke
   coverage.
6. Continue Flask route extraction into blueprints/repositories and add
   component health endpoints. The compatibility factory removes import-time
   infrastructure work but `server.py` is still a route/SQL monolith.
7. Split the two ML modules into feature/pair/train/validate/predict/persist
   stages with immutable data/config/artifact fingerprints.
8. Resolve the locked historical Claude worktree and preserve/discard its owner
   edit explicitly.

### Release-only gates still open

The following cannot be completed safely as a repository-only edit:

- classify and commit the mixed owner working tree into reviewable rollback
  units;
- capture/restore-test APS and Superset backups and catalog inventory;
- provision environment files with mode 0600 and rotate every old credential;
- build the fresh constrained deploy environment;
- inspect/baseline/apply migrations against the intended database;
- deploy the declared SHA to `/opt`, compare/install units, restart
  services, and run HTTP/DB/Superset/read-only production smoke checks;
- manually shadow the nightly DAG, then install/enable/verify the timer and
  failure handler; and
- produce the release manifest and rehearse rollback.

Consequently, Phase 0–2 must still be described as **repository foundations
implemented; production reconciliation and the listed structural slices
open**. The source tree should not be deployed from its current mixed,
uncommitted state.
