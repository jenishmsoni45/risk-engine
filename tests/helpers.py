"""Shared test helpers: the repo root, the Compose command and the stack's host-side addresses."""

from __future__ import annotations

import json
import re
import secrets
import socket
import subprocess
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import psycopg
from confluent_kafka import Consumer, KafkaError, Message, Producer, TopicPartition
from prometheus_client.parser import text_string_to_metric_families
from psycopg import sql
from psycopg.types.json import Jsonb

from risk.features.compute import FEATURE_NAMES
from risk.features.consumer import GROUP_ID
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


def postgres_url() -> str:
    """The stack's database as a URL, for a service run on this machine (DATABASE_URL)."""
    values = read_env_file(env_file())
    return (
        f"postgresql://{values['POSTGRES_USER']}:{values['POSTGRES_PASSWORD']}"
        f"@127.0.0.1:{POSTGRES_HOST_PORT}/{values['POSTGRES_DB']}"
    )


def metric(base_url: str, name: str, **labels: str) -> float:
    """A sample from a service's GET /metrics; 0 when it hasn't been recorded yet."""
    text = httpx.get(f"{base_url}/metrics", timeout=5).text
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            if sample.name == name and sample.labels == labels:
                return float(sample.value)
    return 0.0


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


def count(query: str, params: tuple[Any, ...] = ()) -> int:
    with database() as conn:
        row = conn.execute(query, params).fetchone()
    assert row is not None
    value: int = row[0]
    return value


def iso_ms(epoch_ms: int) -> str:
    """A requestedAt for the given epoch milliseconds, as tally sends it."""
    moment = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=epoch_ms)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def now_ms() -> int:
    return int(time.time() * 1000)


def decision_row(**overrides: Any) -> dict[str, Any]:
    """A complete decisions row (design.md section 2), for inserts that test the schema."""
    now = datetime.now(UTC)
    row: dict[str, Any] = {
        "decision_id": uuid.uuid4(),
        "request_id": unique("schema"),
        "from_account_id": uuid.uuid4(),
        "to_account_id": uuid.uuid4(),
        "amount": 1,
        "currency": "CAD",
        "requested_at": now,
        "decided_at": now,
        "decision": "allow",
        "rule_set_version": 1,
        "rule_set_hash": "0" * 64,
        "fired": Jsonb([]),
        "features": Jsonb(dict.fromkeys(FEATURE_NAMES, 0) | {"amount": 1}),
        "feature_lag_ms": 0,
    }
    row.update(overrides)
    return row


RULE_FIXTURES = ROOT / "tests" / "fixtures" / "rules"


def api_run_args(name: str, rules_file: Path) -> list[str]:
    """`docker compose run` arguments for a second decision API container, from the same image,
    on the stack's Redis and Postgres and started from rules_file, so the API every other test
    uses keeps running. The file's folder is mounted read-only; the container runs as an
    unprivileged user, so the folder and the file must be world-readable."""
    return [
        "run",
        "--rm",
        "--no-deps",
        "--name",
        name,
        "--volume",
        f"{rules_file.parent}:/rules:ro",
        "--env",
        f"RULES_PATH=/rules/{rules_file.name}",
    ]


@contextmanager
def api_with_rules(rules_file: Path) -> Iterator[str]:
    """The base URL of a second decision API started from rules_file, until the block ends."""
    name = unique("risk-engine-api-restarted")
    port = free_port()
    compose(
        *api_run_args(name, rules_file), "--detach", "--publish", f"127.0.0.1:{port}:8000", "api"
    )
    url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                if httpx.get(f"{url}/health", timeout=2).status_code == 200:
                    break
            except httpx.TransportError:
                pass
            assert time.monotonic() < deadline, "the restarted API never became healthy"
            time.sleep(0.2)
        yield url
    finally:
        # --rm removes the container once it has stopped, a moment after `docker stop` returns;
        # waiting for that keeps it out of the next test's `docker compose ps`.
        subprocess.run(["docker", "stop", "--time", "5", name], capture_output=True, check=False)
        wait_until_removed(name)


def wait_until_removed(container: str, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while (
        subprocess.run(
            ["docker", "container", "inspect", container], capture_output=True
        ).returncode
        == 0
    ):
        assert time.monotonic() < deadline, f"{container} was not removed"
        time.sleep(0.2)


def insert_decision(row: dict[str, Any]) -> None:
    """Inserts the row in a transaction that is always rolled back, so nothing is left behind."""
    statement = sql.SQL("insert into decisions ({}) values ({})").format(
        sql.SQL(", ").join(map(sql.Identifier, row)),
        sql.SQL(", ").join(sql.Placeholder() * len(row)),
    )
    with database() as conn, conn.transaction(force_rollback=True):
        conn.execute(statement, tuple(row.values()))


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


def publish(
    messages: Sequence[tuple[bytes | None, bytes | None]], *, partition: int = -1
) -> list[tuple[int, int]]:
    """Publishes (key, value) pairs to ledger.entries, each with a traceparent as tally's relay
    sends (D44), and returns each one's (partition, offset) once the broker has it. Partition -1
    lets the key choose, as tally's producer does."""
    producer = Producer(
        {"bootstrap.servers": KAFKA_HOST_BOOTSTRAP, "acks": "all", "partitioner": "murmur2_random"}
    )
    positions: dict[int, tuple[int, int]] = {}
    failures: list[str] = []

    def reporter(index: int) -> Callable[[KafkaError | None, Message], None]:
        def report(error: KafkaError | None, message: Message) -> None:
            if error is not None:
                failures.append(error.str())
                return
            where, offset = message.partition(), message.offset()
            assert where is not None
            assert offset is not None
            positions[index] = (where, offset)

        return report

    trace = secrets.token_hex(16)
    for index, (key, value) in enumerate(messages):
        producer.produce(
            TOPIC,
            value=value,
            key=key,
            partition=partition,
            headers=[("traceparent", f"00-{trace}-{secrets.token_hex(8)}-03".encode())],
            on_delivery=reporter(index),
        )
    assert producer.flush(15) == 0, "the broker didn't take every message within 15 s"
    assert not failures, failures
    return [positions[index] for index in range(len(messages))]


class ConsumerOffsets:
    """The feature consumer group's committed offsets. The consumer commits only after the writes
    for every message before an offset (D40h), so a committed offset past a message means its
    writes are done. Reading them doesn't join the group."""

    def __init__(self) -> None:
        self.consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_HOST_BOOTSTRAP,
                "group.id": GROUP_ID,
                "enable.auto.commit": False,
            }
        )

    def committed(self) -> dict[int, int]:
        """partition -> next offset to read; a partition never committed is left out."""
        partitions = [TopicPartition(TOPIC, p) for p in range(3)]
        return {
            tp.partition: tp.offset
            for tp in self.consumer.committed(partitions, timeout=10)
            if tp.offset >= 0
        }

    def wait_past(self, positions: Iterable[tuple[int, int]], timeout: float = 30) -> None:
        """Waits until the group has committed past every (partition, offset)."""
        needed: dict[int, int] = {}
        for partition, offset in positions:
            needed[partition] = max(needed.get(partition, 0), offset + 1)
        deadline = time.monotonic() + timeout
        while True:
            committed = self.committed()
            if all(committed.get(p, -1) >= o for p, o in needed.items()):
                return
            assert time.monotonic() < deadline, (
                f"the consumer didn't commit past {needed} within {timeout} s: {committed}"
            )
            time.sleep(0.1)

    def close(self) -> None:
        self.consumer.close()


def wait_until_consumed(positions: Iterable[tuple[int, int]], timeout: float = 30) -> None:
    offsets = ConsumerOffsets()
    try:
        offsets.wait_past(positions, timeout)
    finally:
        offsets.close()
