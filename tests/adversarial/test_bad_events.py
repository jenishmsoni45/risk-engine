"""Malformed events (design.md 1.2, D40i). Phase 3, criterion 6: each malformed class is stored
once in bad_events and counted, and a valid event after it on the same partition is applied.

Each bad message goes to a chosen partition of the stack's ledger.entries, then a valid transfer
to the same partition; the stack's consumer stores the first, counts it on GET /metrics, and
applies the second.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from typing import Any

import pytest
from confluent_kafka import Message

from risk.features.consumer import BadEvents
from risk.features.redis_store import ledger_key, member
from tests.feature_state import STACK_DB, client
from tests.helpers import (
    CONSUMER_METRICS_URL,
    database,
    metric,
    now_ms,
    postgres_conninfo,
    publish,
    wait_until_consumed,
)
from tests.ledger_events import transfer

BAD = "risk_consumer_events_total"


def valid_document() -> dict[str, Any]:
    event = transfer(uuid.uuid4(), uuid.uuid4(), 100, now_ms() * 1000)
    return event.document()


def changed(change: Callable[[dict[str, Any]], None]) -> tuple[bytes | None, bytes | None]:
    """A valid event with one change, keyed by its id."""
    document = valid_document()
    key = document["id"].encode()
    change(document)
    return key, json.dumps(document).encode()


def without(name: str) -> Callable[[dict[str, Any]], None]:
    return lambda d: d.pop(name)


def setting(name: str, value: Any) -> Callable[[dict[str, Any]], None]:
    return lambda d: d.update({name: value})


def line_amount(value: Any) -> Callable[[dict[str, Any]], None]:
    return lambda d: d["lines"][0].update({"amount": value})


def zero_amounts(document: dict[str, Any]) -> None:
    for line in document["lines"]:
        line["amount"] = 0


# design.md 1.2's classes: not JSON, a missing or wrongly typed required field, a key that differs
# from id, an unknown kind, no lines, a zero line amount, lines that don't sum to zero.
# Each value: (key, value) and a fragment of the error stored with it.
CLASSES: dict[str, tuple[Callable[[], tuple[bytes | None, bytes | None]], str]] = {
    "not JSON": (lambda: (str(uuid.uuid4()).encode(), b'{"id": '), "isn't JSON"),
    "not UTF-8": (lambda: (b"k", b"\xff\xfe"), "isn't JSON"),
    "JSON but not an object": (lambda: (b"k", b'["entry"]'), "must be a JSON object"),
    "no value": (lambda: (b"k", None), "has no value"),
    "createdAt missing": (lambda: changed(without("createdAt")), "createdAt"),
    "createdAt not a timestamp": (lambda: changed(setting("createdAt", "yesterday")), "createdAt"),
    "createdAt with an offset": (
        lambda: changed(setting("createdAt", "2026-11-20T15:04:05.123+01:00")),
        "createdAt",
    ),
    "id not a UUID": (lambda: changed(setting("id", 42)), "id must be a UUID"),
    "an amount as a string": (lambda: changed(line_amount("100")), "must be an integer"),
    "an amount as a float": (lambda: changed(line_amount(100.0)), "must be an integer"),
    "an accountId not a UUID": (
        lambda: changed(lambda d: d["lines"][1].update({"accountId": "acct-1"})),
        "accountId",
    ),
    "a key that differs from id": (
        lambda: (str(uuid.uuid4()).encode(), changed(lambda d: None)[1]),
        "key must be the entry id",
    ),
    "no key": (lambda: (None, changed(lambda d: None)[1]), "key must be the entry id"),
    "an unknown kind": (lambda: changed(setting("kind", "refund")), "kind"),
    "no lines": (lambda: changed(setting("lines", [])), "lines must be a non-empty list"),
    "a zero line amount": (lambda: changed(zero_amounts), "must not be zero"),
    "lines that don't sum to zero": (lambda: changed(line_amount(-99)), "sum to zero"),
}


def stored(partition: int, offset: int) -> list[tuple[bytes | None, bytes | None, str]]:
    with database() as conn:
        return conn.execute(
            "select message_key, message_value, error from bad_events "
            "where topic = 'ledger.entries' and kafka_partition = %s and kafka_offset = %s",
            (partition, offset),
        ).fetchall()


@pytest.mark.parametrize("name", CLASSES)
def test_a_bad_event_is_stored_once_and_counted_and_its_partition_flows_on(name: str) -> None:
    make, error = CLASSES[name]
    key, value = make()
    partition = list(CLASSES).index(name) % 3
    good = transfer(uuid.uuid4(), uuid.uuid4(), 1234, now_ms() * 1000)
    before = metric(CONSUMER_METRICS_URL, BAD, outcome="bad")

    ((_, bad_offset),) = publish([(key, value)], partition=partition)
    ((_, good_offset),) = publish([(good.key(), good.value())], partition=partition)
    assert good_offset > bad_offset
    wait_until_consumed([(partition, good_offset)])

    rows = stored(partition, bad_offset)
    assert len(rows) == 1
    stored_key, stored_value, stored_error = rows[0]
    assert (stored_key, stored_value) == (key, value)
    assert error in stored_error
    assert metric(CONSUMER_METRICS_URL, BAD, outcome="bad") == before + 1
    sender = good.lines[0].account
    sent = client(STACK_DB).zscore(ledger_key(sender, "out"), member(1234, str(good.id)))
    assert sent == good.time_ms


def test_a_bad_event_read_again_is_stored_once() -> None:
    """After a crash the consumer may read a bad message again; its position is unique."""
    topic = f"test.bad-events.{uuid.uuid4().hex}"  # a position no real message has
    message = Message(topic=topic, partition=1, offset=7, key=b"k", value=b"nope")
    bad_events = BadEvents(postgres_conninfo())
    bad_events.store(message, "the value isn't JSON")
    bad_events.store(message, "the value isn't JSON")
    with database() as conn:
        count = conn.execute(
            "select count(*) from bad_events where topic = %s", (topic,)
        ).fetchone()
        conn.execute("delete from bad_events where topic = %s", (topic,))
    assert count == (1,)
