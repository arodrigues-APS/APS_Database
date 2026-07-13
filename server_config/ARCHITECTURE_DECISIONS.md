# APS architecture decisions and accepted risks

## Database lifecycle

Forward migrations and repeatable analytical models use separate registries
and ledgers. Existing `020`–`030` names are retained for thesis and
operational traceability. Historical idempotent `common.apply_schema`
remains a compatibility path while DDL moves incrementally.

## Deployment shape

APS remains a one-server monolith with raw psycopg2 and separate PostgreSQL,
Superset, and Redis containers. Microservices, an ORM rewrite, and Airflow are
not current goals.

## Shared metadata table

`baselines_metadata` remains a compatibility hub. New source-specific fields
should use extension tables keyed by metadata ID unless a reviewed cross-source
contract justifies widening it.

## Legacy CV/DPT

The legacy aggregate tables are retained read-only. Their dashboard/model is
explicitly opt-in and labeled as a historical snapshot until a safe canonical,
incremental importer and parity record exist.

## Documentation ownership

Most `docs/` content remains ignored by owner choice. Operational material
needed for release is therefore kept in tracked `Readme.md` and
`server_config/`. The ignored architecture review requires an explicit
backup or force-add decision.

## Remaining monolith boundaries

Flask route/SQL blueprints, ML lifecycle stages, viewer source assets, and the
largest materialized proxy model remain incremental refactors. They must retain
scientific parity and ship as separate reviewed slices.
