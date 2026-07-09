# Agent notes

## Python environment

Use the project virtual environment at `~/aps_venv` (Python 3.12). There is no venv inside the repo.

- Run pipeline modules: `~/aps_venv/bin/python -m aps.<subpackage>.<module>` (e.g. `-m aps.superset.create_proxy_readiness_dashboard`)
- Run tests: `~/aps_venv/bin/python -m pytest`
- Or activate: `source ~/aps_venv/bin/activate`

This machine is also the deploy server for the project.

## Layout

Pipeline code is the installable `aps` package under `src/aps/`
(ingest / seeds / enrich / proxy / ml / superset / viewers / exports;
`aps.paths` holds the repo-root/schema/out anchors). The Flask app
(`server.py`, `wsgi.py`, `data_scraping.py`) stays at the repo root.
Generated artifacts land in `out/` (untracked). `archive/` holds
already-run one-time scripts.
