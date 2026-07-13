import pytest

import server
from aps.config import ConfigurationError, reset_settings_cache


class Scanner:
    def __init__(self):
        self.calls = 0

    def search_results(self, sterms):
        self.calls += 1
        return []


def test_factory_requires_an_explicit_signing_key(monkeypatch):
    monkeypatch.delenv("APS_FLASK_SECRET_KEY", raising=False)
    reset_settings_cache()

    with pytest.raises(ConfigurationError, match="APS_FLASK_SECRET_KEY"):
        server.create_app()


def test_factory_accepts_injected_scanner_without_warming_it():
    scanner = Scanner()

    app = server.create_app(
        secret_key="test-only-secret",
        scanner=scanner,
        config_overrides={"TESTING": True},
    )

    assert app.config["SECRET_KEY"] == "test-only-secret"
    assert app.config["TESTING"] is True
    assert scanner.calls == 0
