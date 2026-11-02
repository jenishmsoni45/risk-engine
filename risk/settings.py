"""Connection settings, read from the environment (design.md 1.6).

Defaults point at the Compose services (D31). DATABASE_URL carries credentials, so it has no
default: Compose builds it from the values in .env or .env.example.
"""

from __future__ import annotations

import os

TOPIC = "ledger.entries"


def redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://redis:6379/0")


def kafka_bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")


def database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    return url
