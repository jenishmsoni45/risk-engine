"""Shared test helpers: the repo root, the Compose command and the stack's host-side addresses."""

from __future__ import annotations

import json
import re
import socket
import subprocess
import time
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx
import psycopg
from confluent_kafka import Consumer, Message, TopicPartition

from risk.settings import TOPIC

ROOT = Path(__file__).resolve().parent.parent

# The stack's published addresses on this machine (D31, D40z).
API_URL = "http://127.0.0.1:8000"
STUB_URL = "http://127.0.0.1:8001"
CONSUMER_METRICS_URL = "http://127.0.0.1:8002"
KAFKA_HOST_BOOTSTRAP = "127.0.0.1:19092"
POSTGRES_HOST_PORT = 5433


def env_file() -> Path:
    """The file Compose reads, chosen the same way as the Makefile: .env, else .env.example."""
    local = ROOT / ".env"
    return local if local.exists() else ROOT / ".env.example"


def read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep:
            raise ValueError(f"{path.name}: not KEY=VALUE: {line!r}")
        values[key.strip()] = value.strip()
    return values


def compose(
    *args: str, check: bool = True, timeout: float = 300
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", "--env-file", str(env_file()), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=check,
        timeout=timeout,
    )


def compose_ps() -> list[dict[str, Any]]:
    """Every container of the project, running or not, as Compose describes it."""
    out = compose("ps", "--all", "--format", "json").stdout.strip()
    if not out:
        return []
    if out.startswith("["):  # older Compose prints one array
        rows: list[dict[str, Any]] = json.loads(out)
        return rows
    return [json.loads(line) for line in out.splitlines()]


def wait_until_healthy(service: str, timeout: float = 60) -> None:
    """Waits for the service's container to report healthy again."""
    name = f"risk-engine-{service}-1"
    deadline = time.monotonic() + timeout
    while True:
        rows = [row for row in compose_ps() if row.get("Name") == name]
        if rows and rows[0].get("Health") == "healthy":
            return
        assert time.monotonic() < deadline, f"{service} not healthy within {timeout} s"
        time.sleep(0.5)


@contextmanager
def paused(service: str) -> Iterator[None]:
    """Pauses a service for the block, then unpauses it and waits until it is healthy again.

    Docker reports a paused-then-unpaused container unhealthy until its next health check, so
    the wait leaves the stack as the next test expects it.
    """
    compose("pause", service)
    try:
        yield
    finally:
        compose("unpause", service)
        wait_until_healthy(service)


def postgres_conninfo(dbname: str | None = None, env: Mapping[str, str] | None = None) -> str:
    values = env if env is not None else read_env_file(env_file())
    return (
        f"host=127.0.0.1 port={POSTGRES_HOST_PORT} user={values['POSTGRES_USER']} "
        f"password={values['POSTGRES_PASSWORD']} dbname={dbname or values['POSTGRES_DB']} "
        "connect_timeout=5"
    )


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port


# --- The decision API ------------------------------------------------------------------------

DECISIONS = ("allow", "hold", "block")
# The canonical form the API writes: lowercase hex, 8-4-4-4-12.
CANONICAL_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
# D40m: a bounded call must end within its timeout plus this margin.
TIMING_MARGIN_S = 0.1
# The one header on a ledger event (D44): a W3C trace context.
TRACEPARENT = re.compile(r"00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}")


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


def decision_request(**overrides: Any) -> dict[str, Any]:
    """A valid request (tally-integration.md 3.1) with a new requestId and new accounts."""
    body: dict[str, Any] = {
        "requestId": unique("test"),
        "fromAccountId": str(uuid.uuid4()),
        "toAccountId": str(uuid.uuid4()),
        "amount": 12500,
        "currency": "CAD",
        "requestedAt": "2026-11-20T15:04:05.123Z",
    }
    body.update(overrides)
    return body


def post_decision(body: Any, base_url: str = API_URL, timeout: float = 5) -> httpx.Response:
    return httpx.post(f"{base_url}/v1/decisions", json=body, timeout=timeout)


def database() -> psycopg.Connection[tuple[Any, ...]]:
    return psycopg.connect(postgres_conninfo(), autocommit=True)


def decision_rows(request_id: str) -> list[tuple[Any, ...]]:
    with database() as conn:
        return conn.execute(
            "select decision_id, decision, rule_set_version, fired from decisions "
            "where request_id = %s",
            (request_id,),
        ).fetchall()


def count(sql: str, params: tuple[Any, ...] = ()) -> int:
    with database() as conn:
        row = conn.execute(sql, params).fetchone()
    assert row is not None
    value: int = row[0]
    return value


# --- ledger.entries --------------------------------------------------------------------------


def text_headers(message: Message) -> list[tuple[str, str]]:
    """A message's headers as (name, text) pairs; the client's types allow str or bytes."""
    headers = message.headers()
    pairs = headers.items() if isinstance(headers, dict) else (headers or [])
    return [(name, raw.decode() if isinstance(raw, bytes) else (raw or "")) for name, raw in pairs]


class TopicTail:
    """Reads ledger.entries from the end it had when created, so only new events are seen."""

    def __init__(self) -> None:
        self.consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_HOST_BOOTSTRAP,
                "group.id": unique("test-tail"),
                "enable.auto.commit": False,
            }
        )
        topic = self.consumer.list_topics(TOPIC, timeout=10).topics[TOPIC]
        start = []
        for partition in topic.partitions:
            _, high = self.consumer.get_watermark_offsets(
                TopicPartition(TOPIC, partition), timeout=10
            )
            start.append(TopicPartition(TOPIC, partition, high))
        self.consumer.assign(start)

    def wait_for(self, key: str, timeout: float = 15) -> Message:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            message = self.consumer.poll(0.2)
            if message is None or message.error() is not None:
                continue
            if message.key() == key.encode():
                return message
        raise AssertionError(f"no event with key {key} on {TOPIC} within {timeout} s")

    def close(self) -> None:
        self.consumer.close()
