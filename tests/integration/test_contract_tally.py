"""The stub's events against events captured from real tally (D25). Phase 2, criterion 12.

The four fixtures (an allowed transfer, a backup-allowed transfer, an entry and a reversal) and
the stub's events parse to the same model, with the same field names, JSON types and timestamp
format, the same one header (D44) and the same partition for a key. How the fixtures were
captured: tests/fixtures/tally/README.md.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from functools import cache
from typing import Any

import httpx
import pytest

from risk.features.events import LedgerEvent, parse_event
from tests.fake_risk import FakeRisk, decided, silent
from tests.helpers import ROOT, STUB_URL, TRACEPARENT, TopicTail, text_headers, unique
from tests.stub_process import running_stub

FIXTURES = ROOT / "tests" / "fixtures" / "tally"
PARTITIONS = 3
# tally writes a Postgres timestamp with Java's ISO_INSTANT: 0, 3 or 6 fraction digits and a Z.
TALLY_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3}|\.\d{6})?Z")
TIMESTAMPS = ("effectiveAt", "createdAt")

# name -> (kind, riskCheck, has a decisionId, has a reversesId)
EXPECTED = {
    "transfer_decided": ("transfer", "decided", True, False),
    "transfer_fallback": ("transfer", "fallback", False, False),
    "entry": ("entry", "none", False, False),
    "reversal": ("reversal", "none", False, True),
}


@dataclass(frozen=True)
class Captured:
    partition: int
    key: str
    headers: list[tuple[str, str]]
    value: str

    def event(self) -> LedgerEvent:
        return parse_event(self.key.encode(), self.value.encode())

    def document(self) -> dict[str, Any]:
        loaded: dict[str, Any] = json.loads(self.value)
        return loaded


@cache
def fixture(name: str) -> Captured:
    loaded = json.loads((FIXTURES / f"{name}.json").read_text())
    assert loaded["topic"] == "ledger.entries"
    return Captured(
        partition=loaded["partition"],
        key=loaded["key"],
        headers=[(header, text) for header, text in loaded["headers"] or []],
        value=loaded["value"],
    )


def java_partition(key: str, partitions: int = PARTITIONS) -> int:
    """Kafka's Java default partitioner for a keyed record: murmur2 of the key, made positive,
    modulo the partition count (org.apache.kafka.common.utils.Utils.murmur2)."""
    data = key.encode()
    mask = 0xFFFFFFFF
    m = 0x5BD1E995
    h = (0x9747B28C ^ len(data)) & mask
    whole = len(data) // 4 * 4
    for i in range(0, whole, 4):
        k = int.from_bytes(data[i : i + 4], "little")
        k = (k * m) & mask
        k ^= k >> 24
        k = (k * m) & mask
        h = (h * m) & mask
        h ^= k
    tail = data[whole:]
    if len(tail) == 3:
        h ^= tail[2] << 16
    if len(tail) >= 2:
        h ^= tail[1] << 8
    if tail:
        h ^= tail[0]
        h = (h * m) & mask
    h ^= h >> 13
    h = (h * m) & mask
    h ^= h >> 15
    return (h & 0x7FFFFFFF) % partitions


def json_type(item: Any) -> str:
    """The JSON type of a parsed value; an int and a float are told apart, as Java reads them."""
    return {
        type(None): "null",
        bool: "boolean",
        int: "integer",
        float: "number",
        str: "string",
        list: "array",
        dict: "object",
    }[type(item)]


def shape(document: dict[str, Any]) -> dict[str, Any]:
    """Field names and JSON types, the lines' included."""
    fields: dict[str, Any] = {name: json_type(item) for name, item in document.items()}
    fields["lines"] = [{n: json_type(i) for n, i in line.items()} for line in document["lines"]]
    return fields


def assert_one_traceparent(headers: list[tuple[str, str]]) -> None:
    assert [name for name, _ in headers] == ["traceparent"]
    assert TRACEPARENT.fullmatch(headers[0][1]), headers


# --- The fixtures ----------------------------------------------------------------------------


@pytest.mark.parametrize("name", EXPECTED)
def test_each_fixture_parses_as_what_it_is(name: str) -> None:
    captured = fixture(name)
    event = captured.event()
    kind, risk_check, has_decision, has_reversed = EXPECTED[name]
    assert event.kind == kind
    assert event.risk_check == risk_check
    assert (event.decision_id is not None) == has_decision
    assert (event.reverses_id is not None) == has_reversed
    for field in TIMESTAMPS:
        assert TALLY_TIMESTAMP.fullmatch(captured.document()[field])
    assert_one_traceparent(captured.headers)
    # Checks java_partition itself against tally's producer.
    assert captured.partition == java_partition(captured.key)


def test_the_reversal_mirrors_the_decided_transfer() -> None:
    transfer, reversal = fixture("transfer_decided").event(), fixture("reversal").event()
    assert reversal.reverses_id == transfer.id
    mirrored = sorted((line.account_id, -line.amount) for line in transfer.lines)
    assert sorted((line.account_id, line.amount) for line in reversal.lines) == mirrored


# --- The stub's events against them ----------------------------------------------------------


def stub_transfer(description: str | None) -> dict[str, Any]:
    body: dict[str, Any] = {
        "fromAccountId": str(uuid.uuid4()),
        "toAccountId": str(uuid.uuid4()),
        "amount": 1500,
        "currency": "CAD",
    }
    if description is not None:
        body["description"] = description
    return body


def published(tail: TopicTail, response: httpx.Response) -> Captured:
    assert response.status_code == 201, response.text
    message = tail.wait_for(response.json()["id"])
    partition = message.partition()
    assert partition is not None
    return Captured(
        partition=partition,
        key=(message.key() or b"").decode(),
        headers=text_headers(message),
        value=(message.value() or b"").decode(),
    )


def stub_decided_event() -> Captured:
    """From the stub container, allowed by the real decision API."""
    tail = TopicTail()
    try:
        body = stub_transfer(fixture("transfer_decided").document()["description"])
        response = httpx.post(
            f"{STUB_URL}/transfers", headers={"Idempotency-Key": unique("contract")}, json=body
        )
        return published(tail, response)
    finally:
        tail.close()


def stub_fallback_event() -> Captured:
    """From a stub whose last reply named allow as the backup, and then got no answer."""
    tail = TopicTail()
    try:
        with FakeRisk() as fake, running_stub(fake.url) as url:
            fake.respond_with(decided("allow", on_timeout="allow"))
            first = httpx.post(
                f"{url}/transfers",
                headers={"Idempotency-Key": unique("contract")},
                json=stub_transfer(None),
            )
            assert first.status_code == 201
            fake.respond_with(silent)
            body = stub_transfer(fixture("transfer_fallback").document()["description"])
            response = httpx.post(
                f"{url}/transfers", headers={"Idempotency-Key": unique("contract")}, json=body
            )
            return published(tail, response)
    finally:
        tail.close()


@pytest.mark.parametrize(
    ("name", "produce"),
    [("transfer_decided", stub_decided_event), ("transfer_fallback", stub_fallback_event)],
)
def test_the_stubs_event_matches_tallys(name: str, produce: Callable[[], Captured]) -> None:
    stub, tally = produce(), fixture(name)

    # The same model: the same kind of entry with the same risk fields.
    kind, risk_check, has_decision, _ = EXPECTED[name]
    for event in (stub.event(), tally.event()):
        assert (event.kind, event.risk_check) == (kind, risk_check)
        assert (event.decision_id is not None) == has_decision
        assert len(event.lines) == 2
        # The sender's line first, then the receiver's.
        assert event.lines[0].amount < 0 < event.lines[1].amount

    # The same field names and JSON types, top level and per line.
    assert shape(stub.document()) == shape(tally.document())
    # The same timestamp format, framing, header and partitioning.
    for field in TIMESTAMPS:
        assert TALLY_TIMESTAMP.fullmatch(stub.document()[field]), stub.document()[field]
    assert stub.key == stub.document()["id"]
    assert_one_traceparent(stub.headers)
    assert stub.partition == java_partition(stub.key)
