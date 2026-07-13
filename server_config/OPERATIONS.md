# APS operations runbook

This runbook describes operator actions. Repository implementation and tests do
not install units, rotate credentials, restore data, or deploy the working tree.

## Pre-release evidence

Record:

- source and target Git SHAs;
- `git status --short` for the source ref;
- Python version and constraints-file checksum;
- installed/running systemd unit content;
- Compose image IDs/digests and PostgreSQL/Superset versions;
- active/next timer state;
- database and materialized-view sizes; and
- published artifact roots.

Required offline gates:

```bash
~/aps_venv/bin/python -m pytest
bash -n scripts/nightly_update_and_ingest.sh
aps db plan
aps models plan proxy-analytics
aps nightly plan
```

Do not release a dirty tree or a script that references untracked
implementation files.

## Backup and restore

Before a release, create custom-format dumps of both the APS data database and
Superset metadata database using the established container backup process.
Record filenames, sizes, timestamps, and checksums.

Restore both dumps into disposable databases/containers. Verify:

- the restore commands exit successfully;
- expected schemas and key relations exist;
- representative row-count/catalog fingerprints match the backup record; and
- the restored Superset metadata can enumerate dashboards/charts.

A backup is not a rehearsed rollback until this restore has succeeded.

## Release sequence

1. Provision `/etc/aps/aps.env` from
   `server_config/aps.env.example` and `superset/.env` from
   `superset/compose.env.example`; use mode 0600.
2. Build a new Python 3.12 environment using
   `requirements/constraints-py312.txt`.
3. Run `aps db status`. On an established database with no forward
   ledger, review and explicitly baseline before any new migration.
4. Apply forward migrations with `aps db migrate`.
5. Build required derived models with
   `aps models build proxy-analytics`.
6. Update the clean `/opt/aps_database/APS_Database` checkout to the
   declared SHA.
7. Compare tracked and installed units, run `systemctl daemon-reload`,
   and restart only affected services.
8. Run application, DB, model, Superset, and explicit read-only production
   smoke checks.
9. Write a release manifest with the final SHA, environment, migrations,
   model-build IDs, unit state, container state, and smoke results.

Credential rotation is a coordinated step after every consumer is configured.
Verify old credentials no longer authenticate.

## Nightly scheduler

The tracked units are `aps-nightly.service` and
`aps-nightly.timer`. The service wrapper creates backups and invokes
the Python DAG. Normal runs do not upgrade containers; use
`APS_UPDATE_INFRASTRUCTURE=1` only in an explicit maintenance window.

Before enabling the timer:

1. run `aps nightly plan`;
2. run one attended manual/shadow execution;
3. inspect `pipeline_runs` and `pipeline_step_runs`;
4. verify backup/log/artifact paths and mount availability; and
5. verify the OnFailure recorder.

Optional-step failures produce `degraded` and a nonzero service result.
Investigate them even when core ingestion succeeded.

## Rollback

Keep the previous code SHA and environment available. Code rollback is safe
only while database changes are backward-compatible. Otherwise use a reviewed
forward fix or the rehearsed data/Superset restore.

Do not delete legacy tables, old models, or backups during the same release
that cuts consumers over to replacements.
