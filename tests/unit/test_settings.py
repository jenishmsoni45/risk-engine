"""Settings from the environment (design.md 1.6): the defaults, and a bad value stops startup."""

from __future__ import annotations

import pytest

from ledger_stub.config import StubConfig
from risk import settings


def test_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "RULES_PATH",
        "DECISION_DEADLINE_MS",
        "KEEP_ALIVE_SECONDS",
        "LATENESS_MARGIN_SECONDS",
        "CONSUMER_METRICS_PORT",
        "CONSUMER_GROUP",
        "CONSUMER_START",
        "STUB_RISK_URL",
        "STUB_RISK_TIMEOUT_MS",
        "STUB_ON_TIMEOUT_DEFAULT",
        "STUB_RELAY_POLL_MS",
        "STUB_RELAY_BATCH",
        "STUB_REPLAY",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATABASE_URL", "postgresql://dummy@db/dummy")
    assert str(settings.rules_path()) == "risk/rules/ruleset.yaml"
    assert settings.decision_deadline_ms() == 60
    assert settings.keep_alive_seconds() == 10
    assert settings.lateness_margin_ms() == 300_000
    assert settings.consumer_metrics_port() == 8002
    assert settings.consumer_group() == "risk-features"
    assert settings.consumer_start() == "earliest"
    assert StubConfig.from_env() == StubConfig(
        database_url="postgresql://dummy@db/dummy",
        kafka_bootstrap=settings.kafka_bootstrap(),
        risk_url="http://api:8000",
        risk_timeout_s=0.1,
        on_timeout_default="hold",
        relay_poll_s=0.2,
        relay_batch=100,
    )


def test_a_lateness_margin_of_zero_is_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LATENESS_MARGIN_SECONDS", "0")
    assert settings.lateness_margin_ms() == 0


def test_a_relay_interval_of_zero_is_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_RELAY_POLL_MS", "0")
    assert settings.stub_relay_poll_ms() == 0


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("DECISION_DEADLINE_MS", "sixty"),
        ("DECISION_DEADLINE_MS", "0"),
        ("KEEP_ALIVE_SECONDS", "-1"),
        ("LATENESS_MARGIN_SECONDS", "-1"),
        ("LATENESS_MARGIN_SECONDS", "5m"),
        ("CONSUMER_METRICS_PORT", "0"),
        ("STUB_RISK_TIMEOUT_MS", "1.5"),
        ("STUB_RELAY_POLL_MS", "-1"),
        ("STUB_RELAY_BATCH", "0"),
        ("STUB_ON_TIMEOUT_DEFAULT", "deny"),
        ("CONSUMER_START", "end"),
        ("STUB_REPLAY", "yes"),
    ],
)
def test_a_bad_value_names_the_variable(
    monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    monkeypatch.setenv(name, value)
    monkeypatch.setenv("DATABASE_URL", "postgresql://dummy@db/dummy")

    def read_every_setting() -> None:
        settings.decision_deadline_ms()
        settings.keep_alive_seconds()
        settings.lateness_margin_ms()
        settings.consumer_metrics_port()
        settings.consumer_start()
        StubConfig.from_env()

    with pytest.raises(RuntimeError, match=name):
        read_every_setting()


def test_database_url_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        settings.database_url()
