"""Settings, read from the environment (design.md 1.6).

Defaults point at the Compose services (D31). DATABASE_URL carries credentials, so it has no
default: Compose builds it from the values in .env or .env.example.
"""

from __future__ import annotations

import os
from pathlib import Path

TOPIC = "ledger.entries"
DECISIONS = ("allow", "hold", "block")


def redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://redis:6379/0")


def kafka_bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")


def database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    return url


def _int(name: str, default: int, minimum: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from None
    if value < minimum:
        raise RuntimeError(f"{name} must be at least {minimum}, got {value}")
    return value


def rules_path() -> Path:
    return Path(os.environ.get("RULES_PATH") or "risk/rules/ruleset.yaml")


def decision_deadline_ms() -> int:
    return _int("DECISION_DEADLINE_MS", 60, 1)


def keep_alive_seconds() -> int:
    return _int("KEEP_ALIVE_SECONDS", 10, 1)


def lateness_margin_ms() -> int:
    """LATENESS_MARGIN_SECONDS (D40c): how much older than watermark - 24 h an event may be and
    still count; also how far behind requestedAt - 24 h attempts are kept."""
    return _int("LATENESS_MARGIN_SECONDS", 300, 0) * 1000


def shadow_model_dir() -> Path | None:
    """SHADOW_MODEL_DIR (D57.5, D58a): the folder of the model scored in shadow, `ml/model` when
    unset. Set to empty, it turns shadow scoring off."""
    value = os.environ.get("SHADOW_MODEL_DIR")
    if value is None:
        return Path("ml/model")
    return Path(value) if value else None


def shadow_timeout_ms() -> int:
    """SHADOW_TIMEOUT_MS (D57.1): how long a decision waits for the model's score."""
    return _int("SHADOW_TIMEOUT_MS", 10, 1)


def consumer_metrics_port() -> int:
    """The consumer's GET /metrics port (D40z)."""
    return _int("CONSUMER_METRICS_PORT", 8002, 1)


CONSUMER_STARTS = ("earliest", "latest")


def consumer_group() -> str:
    """CONSUMER_GROUP (D48.6): the consumer group the feature consumer joins."""
    return os.environ.get("CONSUMER_GROUP") or "risk-features"


def consumer_start() -> str:
    """CONSUMER_START (D48.6): where a group with no committed offsets starts, `earliest` (the
    oldest event kept) or `latest` (the topic's end, for make e2e)."""
    value = os.environ.get("CONSUMER_START") or "earliest"
    if value not in CONSUMER_STARTS:
        raise RuntimeError(f"CONSUMER_START must be one of {CONSUMER_STARTS}, got {value!r}")
    return value


def stub_risk_url() -> str:
    return os.environ.get("STUB_RISK_URL") or "http://api:8000"


def stub_risk_timeout_ms() -> int:
    return _int("STUB_RISK_TIMEOUT_MS", 100, 1)


def stub_on_timeout_default() -> str:
    value = os.environ.get("STUB_ON_TIMEOUT_DEFAULT") or "hold"
    if value not in DECISIONS:
        raise RuntimeError(f"STUB_ON_TIMEOUT_DEFAULT must be one of {DECISIONS}, got {value!r}")
    return value


def stub_relay_poll_ms() -> int:
    return _int("STUB_RELAY_POLL_MS", 200, 0)


def stub_relay_batch() -> int:
    return _int("STUB_RELAY_BATCH", 100, 1)


def stub_replay() -> bool:
    """STUB_REPLAY (D40u): `1` turns on replay mode (design.md 1.3); unset, empty or `0` leaves it
    off. Anything else stops the stub, so a typo never starts a replay or a normal stub by
    mistake."""
    value = os.environ.get("STUB_REPLAY") or "0"
    if value not in ("0", "1"):
        raise RuntimeError(f"STUB_REPLAY must be 0 or 1, got {value!r}")
    return value == "1"
