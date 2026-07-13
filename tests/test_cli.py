from io import StringIO

import aps.cli as cli


def test_db_plan_runs_without_opening_a_database(monkeypatch):
    output = StringIO()

    def unexpected_connection():
        raise AssertionError("offline plan must not open a database connection")

    monkeypatch.setattr(cli, "get_connection", unexpected_connection)

    assert cli.main(["db", "plan"], output=output) == 0
    assert "000_device_library.sql" in output.getvalue()


def test_model_dry_run_runs_without_opening_a_database(monkeypatch):
    output = StringIO()

    def unexpected_connection():
        raise AssertionError("dry-run must not open a database connection")

    monkeypatch.setattr(cli, "get_connection", unexpected_connection)

    assert cli.main(["models", "build", "proxy-analytics", "--dry-run"], output=output) == 0
    rendered = output.getvalue()
    assert "model: proxy-analytics" in rendered
    assert "schema/029_proxy_viz_support.sql" in rendered
