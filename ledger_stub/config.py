"""The stub's settings (design.md 1.6). Tests build one directly to run a stub in-process."""

from __future__ import annotations

from dataclasses import dataclass

from risk import settings


@dataclass(frozen=True)
class StubConfig:
    database_url: str
    kafka_bootstrap: str
    risk_url: str
    risk_timeout_s: float
    on_timeout_default: str
    relay_poll_s: float
    relay_batch: int
    # Replay mode (design.md 1.3, D40u, D40v): off unless STUB_REPLAY=1.
    replay: bool = False

    @classmethod
    def from_env(cls) -> StubConfig:
        return cls(
            database_url=settings.database_url(),
            kafka_bootstrap=settings.kafka_bootstrap(),
            risk_url=settings.stub_risk_url(),
            risk_timeout_s=settings.stub_risk_timeout_ms() / 1000,
            on_timeout_default=settings.stub_on_timeout_default(),
            relay_poll_s=settings.stub_relay_poll_ms() / 1000,
            relay_batch=settings.stub_relay_batch(),
            replay=settings.stub_replay(),
        )
