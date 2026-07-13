"""Small explicit database operations used by the pipeline manifest."""

from __future__ import annotations

import argparse

from aps.db_config import get_connection


def refresh_baselines_run_max_current() -> None:
    """Refresh the one retained materialized baseline summary explicitly."""
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("REFRESH MATERIALIZED VIEW baselines_run_max_current")
        conn.commit()
    print("refreshed baselines_run_max_current")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "operation",
        choices=("refresh-baselines-run-max-current",),
    )
    args = parser.parse_args()
    if args.operation == "refresh-baselines-run-max-current":
        refresh_baselines_run_max_current()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
