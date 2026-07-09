#!/usr/bin/env python3
"""Extract per-sample pulse history for electrical stress waveforms.

The proxy-readiness views use this table as context only: repetition history is
reported next to SELC/cumulative irradiation targets but is not a distance axis.

Rows are intentionally emitted only when a waveform has an explicit sequence
counter in metadata or in the filename/path. That keeps duration sweeps and
standalone waveform captures from being mislabeled as cumulative exposure.
"""

from __future__ import annotations

import argparse
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from psycopg2.extras import RealDictCursor, execute_values

try:
    from aps.db_config import get_connection
except ModuleNotFoundError:  # pragma: no cover - used when imported as package.
    from aps.db_config import get_connection


DDL_SQL = """
CREATE TABLE IF NOT EXISTS stress_pulse_history (
    metadata_id integer PRIMARY KEY REFERENCES baselines_metadata(id) ON DELETE CASCADE,
    pulse_index integer,
    pulse_count_in_sequence integer,
    sequence_key text,
    cumulative_energy_j double precision,
    basis text,
    provenance text,
    updated_at timestamp with time zone NOT NULL DEFAULT now()
);

ALTER TABLE stress_pulse_history
    ADD COLUMN IF NOT EXISTS pulse_index integer,
    ADD COLUMN IF NOT EXISTS pulse_count_in_sequence integer,
    ADD COLUMN IF NOT EXISTS sequence_key text,
    ADD COLUMN IF NOT EXISTS cumulative_energy_j double precision,
    ADD COLUMN IF NOT EXISTS basis text,
    ADD COLUMN IF NOT EXISTS provenance text,
    ADD COLUMN IF NOT EXISTS updated_at timestamp with time zone NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_stress_pulse_history_sequence
    ON stress_pulse_history(sequence_key, pulse_count_in_sequence);
CREATE INDEX IF NOT EXISTS idx_stress_pulse_history_basis
    ON stress_pulse_history(basis);
"""

FETCH_SQL = """
SELECT
    id,
    data_source,
    measurement_category,
    device_type,
    device_id,
    sample_group,
    filename,
    csv_path,
    sc_voltage_v,
    sc_duration_us,
    sc_sequence_num,
    sc_condition_label,
    avalanche_mode,
    avalanche_energy_j,
    avalanche_peak_current_a,
    avalanche_gate_bias_v,
    avalanche_gate_bias_raw,
    avalanche_shot_index,
    avalanche_condition_label,
    avalanche_inductance_mh,
    avalanche_temperature_c
FROM baselines_metadata
WHERE data_source = 'avalanche'
   OR (data_source = 'sc_ruggedness' AND measurement_category = 'SC_Waveform')
ORDER BY data_source, device_type NULLS LAST, sample_group NULLS LAST,
         device_id NULLS LAST, id
"""

UPSERT_SQL = """
INSERT INTO stress_pulse_history (
    metadata_id,
    pulse_index,
    pulse_count_in_sequence,
    sequence_key,
    cumulative_energy_j,
    basis,
    provenance
)
VALUES %s
ON CONFLICT (metadata_id) DO UPDATE SET
    pulse_index = EXCLUDED.pulse_index,
    pulse_count_in_sequence = EXCLUDED.pulse_count_in_sequence,
    sequence_key = EXCLUDED.sequence_key,
    cumulative_energy_j = EXCLUDED.cumulative_energy_j,
    basis = EXCLUDED.basis,
    provenance = EXCLUDED.provenance,
    updated_at = now()
"""


@dataclass(frozen=True)
class PulseHistoryRow:
    metadata_id: int
    source: str
    pulse_index: int
    pulse_count_in_sequence: int
    sequence_key: str
    cumulative_energy_j: float | None
    basis: str
    provenance: str

    def db_tuple(self) -> tuple[Any, ...]:
        return (
            self.metadata_id,
            self.pulse_index,
            self.pulse_count_in_sequence,
            self.sequence_key,
            self.cumulative_energy_j,
            self.basis,
            self.provenance,
        )


def finite_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result


def finite_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return result


def compact_number(value: Any) -> str:
    number = finite_float(value)
    if number is None:
        return "na"
    return f"{number:.6g}"


def path_text(*values: Any) -> str:
    return "/".join(str(value) for value in values if value)


def parse_avalanche_pulse_index(path_or_name: str | None,
                                metadata_index: Any = None) -> tuple[int | None, str | None]:
    """Return the avalanche pulse counter and its evidence basis.

    The Selam HDF5 names encode gate bias and a five-digit shot counter in a
    fused token (for example Vg-1000001 means Vg=-10 V, shot 1). This mirrors
    ingestion_avalanche.py rather than taking the whole numeric tail literally.
    """
    index = finite_int(metadata_index)
    if index is not None:
        return index, "avalanche_shot_index_metadata"

    if not path_or_name:
        return None, None

    stem = Path(str(path_or_name).replace("\\", "/")).stem

    for raw_token in re.findall(r"Vg([^_/\\.]+)", stem, re.IGNORECASE):
        token = raw_token.strip()
        token_body = token[1:] if token.startswith(("+", "-")) else token
        if re.fullmatch(r"\d{6,}", token_body):
            return int(token_body[-5:]), "avalanche_filename_vg_counter"

    match = re.search(r"(?:^|_)(?:\d+(?:[p.]\d+)?)A(\d{5,})(?:_|$)", stem,
                      re.IGNORECASE)
    if match:
        return int(match.group(1)), "avalanche_filename_current_counter"

    match = re.search(r"(?:^|_)(\d+\.\d{1,3})(\d{5})(?:_|$)", stem)
    if match:
        return int(match.group(2)), "avalanche_filename_fused_energy_counter"

    match = re.search(r"(\d{5,})$", stem)
    if match:
        return int(match.group(1)), "avalanche_filename_tail_counter"

    return None, None


def parse_sc_pulse_index(path_or_name: str | None,
                         metadata_index: Any = None) -> tuple[int | None, str | None]:
    """Return the SC pulse counter when the filename/path carries one."""
    index = finite_int(metadata_index)
    if index is not None:
        return index, "sc_sequence_num_metadata"

    if not path_or_name:
        return None, None

    text = str(path_or_name).replace("\\", "/")
    basename = Path(text).name
    search_space = f"/{text}/{basename}"

    match = re.search(r"(?:^|[^A-Za-z0-9])pulse[_-]?(\d+)(?:[^A-Za-z0-9]|$)",
                      search_space, re.IGNORECASE)
    if match:
        return int(match.group(1)), "sc_filename_pulse_counter"

    match = re.search(r"(?:^|/)(\d+)_after\d+V", search_space, re.IGNORECASE)
    if match:
        return int(match.group(1)), "sc_filename_after_counter"

    ordinals = {
        "first": 1,
        "second": 2,
        "third": 3,
        "fourth": 4,
        "fifth": 5,
        "sixth": 6,
        "seventh": 7,
        "eighth": 8,
    }
    for component in search_space.split("/"):
        lowered = component.lower()
        for word, ordinal in ordinals.items():
            if word in lowered:
                return ordinal, "sc_filename_ordinal_counter"

    return None, None


def source_name(row: dict[str, Any]) -> str | None:
    data_source = row.get("data_source")
    if data_source == "avalanche":
        return "avalanche"
    if data_source == "sc_ruggedness":
        return "sc"
    return None


def physical_sample_key(row: dict[str, Any]) -> str:
    return str(
        row.get("sample_group")
        or row.get("device_id")
        or row.get("filename")
        or f"metadata_{row.get('id')}"
    )


def sequence_key(row: dict[str, Any], source: str) -> str:
    """Build the cumulative-history grouping key.

    The key is per physical sample, not per test condition. Repetition damage is
    a sample history, so changing energy or gate bias still contributes to the
    cumulative count for that sample.
    """
    device = row.get("device_type") or "unknown_device"
    sample = physical_sample_key(row)
    return "|".join((source, str(device), sample))


def row_pulse_energy_j(row: dict[str, Any], source: str) -> float | None:
    if source == "avalanche":
        energy = finite_float(row.get("avalanche_energy_j"))
        if energy is not None and energy > 0.0:
            return energy
    return None


def pulse_index_for_row(row: dict[str, Any], source: str) -> tuple[int | None, str | None]:
    combined_path = path_text(row.get("csv_path"), row.get("filename"))
    if source == "avalanche":
        return parse_avalanche_pulse_index(
            combined_path,
            metadata_index=row.get("avalanche_shot_index"),
        )
    if source == "sc":
        return parse_sc_pulse_index(
            combined_path,
            metadata_index=row.get("sc_sequence_num"),
        )
    return None, None


def build_history_rows(rows: Iterable[dict[str, Any]]) -> list[PulseHistoryRow]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for raw in rows:
        row = dict(raw)
        source = source_name(row)
        if source is None:
            continue
        pulse_index, basis = pulse_index_for_row(row, source)
        if pulse_index is None or basis is None:
            continue
        row["_source"] = source
        row["_pulse_index"] = pulse_index
        row["_basis"] = basis
        row["_pulse_energy_j"] = row_pulse_energy_j(row, source)
        row["_sequence_key"] = sequence_key(row, source)
        grouped[row["_sequence_key"]].append(row)

    history_rows: list[PulseHistoryRow] = []
    for key, sequence_rows in grouped.items():
        ordered = sorted(
            sequence_rows,
            key=lambda row: (
                row["_pulse_index"],
                finite_int(row.get("id")) or 0,
            ),
        )
        running_energy = 0.0
        cumulative_energy_complete = True
        for count, row in enumerate(ordered, start=1):
            pulse_energy = row["_pulse_energy_j"]
            if pulse_energy is None:
                cumulative_energy_complete = False
            elif cumulative_energy_complete:
                running_energy += pulse_energy
            cumulative_energy = running_energy if cumulative_energy_complete else None
            provenance = (
                "extract_stress_pulse_history.py; "
                f"source={row['_source']}; key={key}; basis={row['_basis']}"
            )
            history_rows.append(
                PulseHistoryRow(
                    metadata_id=int(row["id"]),
                    source=row["_source"],
                    pulse_index=int(row["_pulse_index"]),
                    pulse_count_in_sequence=count,
                    sequence_key=key,
                    cumulative_energy_j=cumulative_energy,
                    basis=row["_basis"],
                    provenance=provenance,
                )
            )

    return sorted(history_rows, key=lambda row: (row.sequence_key, row.pulse_count_in_sequence, row.metadata_id))


def fetch_rows(conn, limit: int | None = None) -> list[dict[str, Any]]:
    sql = FETCH_SQL
    params: tuple[Any, ...] = ()
    if limit is not None:
        sql += " LIMIT %s"
        params = (limit,)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL_SQL)


def write_history_rows(conn, rows: list[PulseHistoryRow], rebuild: bool) -> None:
    with conn.cursor() as cur:
        if rebuild:
            cur.execute("DELETE FROM stress_pulse_history")
        if rows:
            execute_values(cur, UPSERT_SQL, [row.db_tuple() for row in rows], page_size=1000)


def print_summary(rows: list[PulseHistoryRow], source_total: int, dry_run: bool) -> None:
    print(f"Dry run: {dry_run}")
    print(f"Source waveform rows scanned: {source_total}")
    print(f"Pulse history rows built: {len(rows)}")
    by_source = Counter(row.source for row in rows)
    by_basis = Counter(row.basis for row in rows)
    print("Rows by source:")
    for source, count in sorted(by_source.items()):
        print(f"  {source}: {count}")
    print("Rows by basis:")
    for basis, count in sorted(by_basis.items()):
        print(f"  {basis}: {count}")
    sequence_lengths = Counter(row.sequence_key for row in rows)
    if sequence_lengths:
        longest = sorted(sequence_lengths.items(), key=lambda item: item[1], reverse=True)[:5]
        print("Longest sequences:")
        for key, count in longest:
            print(f"  {key}: {count}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Delete existing stress_pulse_history rows before inserting rebuilt rows.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and summarize without writing to the database.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit source rows for parser debugging.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    with get_connection() as conn:
        ensure_schema(conn)
        rows = fetch_rows(conn, limit=args.limit)
        history_rows = build_history_rows(rows)
        print_summary(history_rows, len(rows), dry_run=args.dry_run)
        if args.dry_run:
            conn.rollback()
            return 0
        write_history_rows(conn, history_rows, rebuild=args.rebuild)
        conn.commit()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
