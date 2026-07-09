import unittest

from data_processing_scripts import common


PLAIN_SQL = "CREATE TABLE IF NOT EXISTS demo (id INT);\n"
PIPELINE_SQL = (
    "-- apply_schema: pipeline-owned\n"
    "CREATE TABLE IF NOT EXISTS pipeline_demo (id INT);\n"
)


class FakeCursor:
    """Simulates just enough of a cursor for apply_schema's ledger writes."""

    def __init__(self, ledger):
        self.ledger = ledger  # filename -> list of [id, checksum]
        self.executed_sql = []
        self.inserts = []
        self.touches = []
        self._fetchone = None

    def execute(self, sql, params=None):
        self.executed_sql.append(sql)
        if sql == common.SCHEMA_LEDGER_SELECT_SQL:
            rows = self.ledger.get(params[0], [])
            self._fetchone = tuple(rows[-1]) if rows else None
        elif sql == common.SCHEMA_LEDGER_INSERT_SQL:
            filename, checksum = params
            rows = self.ledger.setdefault(filename, [])
            next_id = 1 + max(
                (r[0] for lst in self.ledger.values() for r in lst), default=0
            )
            rows.append([next_id, checksum])
            self.inserts.append((filename, checksum))
        elif sql == common.SCHEMA_LEDGER_TOUCH_SQL:
            self.touches.append(params[0])

    def fetchone(self):
        return self._fetchone

    def close(self):
        pass


class FakeConn:
    def __init__(self, ledger=None):
        self.ledger = ledger if ledger is not None else {}
        self.cursors = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        cur = FakeCursor(self.ledger)
        self.cursors.append(cur)
        return cur

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class ApplySchemaLedgerTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        from pathlib import Path

        self._tmp = tempfile.TemporaryDirectory()
        self.schema_dir = Path(self._tmp.name)
        (self.schema_dir / "001_core.sql").write_text(PLAIN_SQL)
        (self.schema_dir / "002_pipeline.sql").write_text(PIPELINE_SQL)

    def tearDown(self):
        self._tmp.cleanup()

    def test_ledger_table_created_before_files_run(self):
        conn = FakeConn()
        common.apply_schema(conn, schema_dir=self.schema_dir)

        cur = conn.cursors[0]
        self.assertEqual(cur.executed_sql[0], common.SCHEMA_LEDGER_TABLE_SQL)
        self.assertIn(PLAIN_SQL, cur.executed_sql)
        self.assertEqual(conn.commits, 1)

    def test_first_apply_inserts_one_row_per_executed_file(self):
        conn = FakeConn()
        common.apply_schema(conn, schema_dir=self.schema_dir)

        cur = conn.cursors[0]
        self.assertEqual(
            cur.inserts,
            [("001_core.sql", common.schema_checksum(PLAIN_SQL))],
        )
        self.assertEqual(cur.touches, [])

    def test_pipeline_owned_file_skipped_and_not_recorded(self):
        conn = FakeConn()
        common.apply_schema(conn, schema_dir=self.schema_dir)

        cur = conn.cursors[0]
        self.assertNotIn(PIPELINE_SQL, cur.executed_sql)
        self.assertNotIn("002_pipeline.sql", conn.ledger)

    def test_included_pipeline_file_executed_and_recorded(self):
        conn = FakeConn()
        common.apply_schema(
            conn, include_pipeline={"002_pipeline.sql"}, schema_dir=self.schema_dir
        )

        cur = conn.cursors[0]
        self.assertIn(PIPELINE_SQL, cur.executed_sql)
        self.assertIn(
            ("002_pipeline.sql", common.schema_checksum(PIPELINE_SQL)),
            cur.inserts,
        )

    def test_reapply_unchanged_touches_instead_of_inserting(self):
        conn = FakeConn()
        common.apply_schema(conn, schema_dir=self.schema_dir)
        common.apply_schema(conn, schema_dir=self.schema_dir)

        second = conn.cursors[1]
        self.assertEqual(second.inserts, [])
        self.assertEqual(len(second.touches), 1)
        self.assertEqual(len(conn.ledger["001_core.sql"]), 1)

    def test_edited_file_gets_a_new_ledger_row(self):
        conn = FakeConn()
        common.apply_schema(conn, schema_dir=self.schema_dir)
        (self.schema_dir / "001_core.sql").write_text(
            PLAIN_SQL + "-- edited\n"
        )
        common.apply_schema(conn, schema_dir=self.schema_dir)

        self.assertEqual(len(conn.ledger["001_core.sql"]), 2)
        second = conn.cursors[1]
        self.assertEqual(second.touches, [])

    def test_failure_rolls_back_and_raises(self):
        class ExplodingCursor(FakeCursor):
            def execute(self, sql, params=None):
                if sql == PLAIN_SQL:
                    raise RuntimeError("boom")
                super().execute(sql, params)

        class ExplodingConn(FakeConn):
            def cursor(self):
                cur = ExplodingCursor(self.ledger)
                self.cursors.append(cur)
                return cur

        conn = ExplodingConn()
        with self.assertRaises(RuntimeError):
            common.apply_schema(conn, schema_dir=self.schema_dir)
        self.assertEqual(conn.rollbacks, 1)
        self.assertEqual(conn.commits, 0)
        self.assertEqual(conn.ledger, {})


class SchemaStatusTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        from pathlib import Path

        self._tmp = tempfile.TemporaryDirectory()
        self.schema_dir = Path(self._tmp.name)
        (self.schema_dir / "001_core.sql").write_text(PLAIN_SQL)

    def tearDown(self):
        self._tmp.cleanup()

    def _status_conn(self, rows):
        class StatusCursor(FakeCursor):
            def execute(self, sql, params=None):
                self.executed_sql.append(sql)
                self._rows = rows

            def fetchall(self):
                return self._rows

        class StatusConn(FakeConn):
            def cursor(self):
                cur = StatusCursor(self.ledger)
                self.cursors.append(cur)
                return cur

        return StatusConn()

    def test_in_sync_and_never_recorded_and_missing_file(self):
        applied = common.schema_checksum(PLAIN_SQL)
        conn = self._status_conn(
            [
                ("001_core.sql", applied, "2026-07-09"),
                ("gone.sql", "deadbeef", "2026-01-01"),
            ]
        )
        status = dict(
            (name, state)
            for name, state, _ in common.schema_status(conn, schema_dir=self.schema_dir)
        )
        self.assertEqual(status["001_core.sql"], "in_sync")
        self.assertEqual(status["gone.sql"], "missing_file")

    def test_edited_since_apply(self):
        conn = self._status_conn([("001_core.sql", "stale", "2026-07-09")])
        status = common.schema_status(conn, schema_dir=self.schema_dir)
        self.assertEqual(status[0][1], "edited_since_apply")

    def test_empty_ledger_reports_never_recorded(self):
        class BrokenCursor(FakeCursor):
            def execute(self, sql, params=None):
                raise RuntimeError("relation schema_migrations does not exist")

        class BrokenConn(FakeConn):
            def cursor(self):
                cur = BrokenCursor(self.ledger)
                self.cursors.append(cur)
                return cur

        conn = BrokenConn()
        status = common.schema_status(conn, schema_dir=self.schema_dir)
        self.assertEqual(status, [("001_core.sql", "never_recorded", None)])
        self.assertEqual(conn.rollbacks, 1)


if __name__ == "__main__":
    unittest.main()
