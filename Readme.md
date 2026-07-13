# APS Database

APS Database is an internal measurement-data system for ingesting semiconductor
experiments, deriving damage/proxy/ML models, publishing Superset dashboards and
self-contained viewers, and serving the raw-data administration/search UI.

The code is a single deployable system with explicit internal boundaries:

```text
source files
    -> aps.ingest / aps.seeds
    -> aps.enrich / aps.ml
    -> aps.db.models
    -> aps.superset / aps.viewers / aps.exports
```

The Flask application remains at the repository root. Pipeline code is the
installable `aps` package under `src/aps/`. Generated artifacts belong in
`out/`; already-run one-time scripts belong in `archive/`.

## Environment and installation

Production and local commands must run from an installed environment. On this
host the maintained development environment is `~/aps_venv`; the deployed
environment is `/opt/aps_database/venv`.

To create a clean Python 3.12 environment:

```bash
python3.12 -m venv /tmp/aps-venv
/tmp/aps-venv/bin/python -m pip install --upgrade pip
/tmp/aps-venv/bin/python -m pip install \
  -c requirements/constraints-py312.txt \
  -e ".[server,pipeline,ml,dev]"
```

Application modules never install packages at runtime. A missing optional
dependency fails with an actionable error.

## Configuration

All APS processes use `aps.config.Settings`. The supported profiles are
`development`, `test`, and `production`; reaching a localhost
database never selects production implicitly.

The versioned service template is
`server_config/aps.env.example`. Production copies it to
`/etc/aps/aps.env`, replaces every `change-me` value, and restricts the
file to the service owner. The Flask and nightly units load that same file.

Superset Compose has a separate template at
`superset/compose.env.example`, copied to `superset/.env`. Compose
signing/database secrets are required environment variables; the APS data
PostgreSQL port is bound to loopback by default.

Local development may set `APS_ENV_FILE` to an explicit environment file.
The package never discovers a repository `.env` implicitly. Inspect the active
contract without printing secret values:

```bash
aps config show
```

## Command line

The stable command boundary is `aps`:

```bash
aps db plan
aps db status
aps db migrate
aps models list
aps models plan proxy-analytics
aps models build proxy-analytics --dry-run
aps nightly plan
```

Legacy `python -m aps.<subpackage>.<module>` commands remain compatibility
entry points while individual flows migrate.

### Database ownership

APS deliberately separates two database lifecycles:

1. Forward structural migrations are immutable, checksum-protected, and applied
   once through `aps db migrate`. An advisory lock prevents concurrent
   migrators.
2. Repeatable analytical models have declared SQL assets, dependencies,
   expected outputs, timing/statistics, and a separate `aps_model_builds`
   ledger.

An existing database with public objects but no `aps_forward_migrations`
ledger is never assumed blank. Review `aps db plan` and use
`--baseline-existing-through 026_irradiation_energy_windows.sql` only after
verifying that exact historical boundary; newer migrations still execute.

The proxy analytical bundle (`schema/025`, `028`, and `029`) has
one Python owner: `aps.db.models`. Extraction and dashboards do not rebuild it.
The nightly pipeline builds it once after upstream enrichment.

## Nightly pipeline

`scripts/nightly_update_and_ingest.sh` is the OS wrapper. It owns:

- the process lock;
- service health checks;
- PostgreSQL and Superset-metadata dumps;
- log/backup retention; and
- invocation of `aps nightly run`.

The data DAG itself lives in `aps.pipelines.nightly`. Each named step has
dependencies and a critical/optional policy. Runs and step attempts are written
to `pipeline_runs` and `pipeline_step_runs`. Critical failure stops
the DAG. Optional viewer/export/publish failure yields a degraded run and a
nonzero service result instead of disappearing behind shell `|| true`.

Inspect ordering without touching PostgreSQL:

```bash
aps nightly plan
aps nightly plan --until dashboard-proxy-readiness
```

Unsafe partial selections that omit dependencies are rejected. Normal nightly
ingestion does not pull or recreate containers. Infrastructure updates require
the explicit maintenance flag `APS_UPDATE_INFRASTRUCTURE=1`.

## Tests and CI

The default suite is offline and excludes the production-smoke tier:

```bash
~/aps_venv/bin/python -m pytest
```

Available tiers:

- `unit`: infrastructure-free behavior (the default);
- `integration`: disposable PostgreSQL or other isolated services; and
- `production_smoke`: explicit read-only checks against production.

Production smoke tests require `APS_RUN_PRODUCTION_SMOKE=1` and configured
read-only credentials. CI installs from the Python 3.12 constraints baseline,
runs the offline suite, runs the integration tier against disposable
PostgreSQL 15, and lints the architecture-foundation files with Ruff.
Architecture tests prevent Superset-to-ingestion imports, runtime pip calls,
client/export-policy inversion, and multiple Python owners for proxy model SQL.

## Superset, viewers, and exports

Dashboard builders consume prepared database relations and never execute model
SQL. The shared Superset client applies connect/read timeouts, retries only safe
GET requests, requires JWT and CSRF authentication, and raises typed errors for
non-success responses. Mutating requests are not retried because their remote
outcome may be unknown after a timeout.

Chart descriptions belong to presentation support, not the HTTP client. Local
CSV/PNG artifact export remains a separate downstream concern.

Viewer artifacts are generated under `out/`. The interactive
damage-signature viewer is published atomically by a recorded optional pipeline
step; generation or publication failure marks the run degraded.

## Flask raw-data application

`server.create_app` is the compatibility application factory. Importing
`server` performs no database migration and no NAS/data-tree scan. The
factory requires `APS_FLASK_SECRET_KEY`, accepts an injected scanner for
tests, and resolves repository files independently of the process working
directory. `wsgi.py` creates the deployed app through this boundary.

Nginx Basic Auth is the current authorization boundary for the raw-data and
Superset sites. The application must not be exposed directly around nginx.
Blueprint/repository extraction is still incremental; route SQL remains a
known monolith boundary.

## Release and recovery

Repository changes are not a deployment. A coordinated release should:

1. start from a clean, committed ref and record its SHA;
2. create and verify data and Superset-metadata backups;
3. build a fresh constrained environment;
4. inspect/apply forward migrations;
5. build required analytical models;
6. update the `/opt/aps_database/APS_Database` checkout;
7. compare/install the tracked units and restart only affected services;
8. run HTTP, database, Superset, and production-smoke checks; and
9. emit a release manifest containing code, environment, database/model, and
   service state.

Container-image upgrades and credential rotation are separate maintenance
windows. Code rollback retains the prior SHA/environment; a non-backward-
compatible database change may require a forward fix or a tested restore.
Never treat an untested dump as a complete rollback procedure.

Tracked unit templates are in `server_config/`. They target the `/opt`
checkout and load `/etc/aps/aps.env`. Installing/enabling units, rotating
credentials, restoring backups, and deploying are operator actions and are not
performed by repository tests.

## Data search naming

The raw-data search UI indexes the directory configured by `config.ini`.
Useful paths include a person/owner, device or sample identifier, measurement
technique, and experimental detail, for example:

```text
data/smith/C2M0080120D/iv/drain-source
data/doe/dlts/Ascatron-150um-EPI-41
```

Search results are cached for three minutes and populated lazily on the first
request rather than during WSGI import.

## Current accepted limits

- The system remains a one-server monolith using raw psycopg2.
- Existing numbered SQL assets are retained for traceability.
- `baselines_metadata` remains a broad compatibility table.
- Legacy CV/DPT tables remain read-only until a canonical importer is built.
- Most personal review material under `docs/` remains ignored by owner
  choice and needs an explicit backup/force-add decision.
- Flask blueprint/repository extraction, ML lifecycle splitting, viewer asset
  extraction, and large-model performance redesign remain follow-up work.
