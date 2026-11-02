"""The ledger event parser (design.md 1.2): what it accepts, and each kind of bad event.

Phase 3's consumer stores bad events in bad_events; its tests send these through Kafka.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from risk.features.events import BadEventError, parse_event

ENTRY_ID = "d1729179-ad20-4af4-8398-186cefb7186e"
ALICE = "1f5295d5-2cda-409a-bfa1-6ea1af6165dc"
BOB = "ab5231da-bfa7-4e43-bcbd-d043668f684c"


def event(**overrides: Any) -> dict[str, Any]:
    document: dict[str, Any] = {
        "id": ENTRY_ID,
        "kind": "transfer",
        "reversesId": None,
        "description": "rent",
        "effectiveAt": "2026-11-02T16:00:23.696136Z",
        "createdAt": "2026-11-02T16:00:23.696136Z",
        "decisionId": "1a17e4ef-2d31-4550-a19a-9ac41e1b73a3",
        "riskCheck": "decided",
        "lines": [
            {"id": 1, "accountId": ALICE, "amount": -1500, "currency": "CAD"},
            {"id": 2, "accountId": BOB, "amount": 1500, "currency": "CAD"},
        ],
    }
    document.update(overrides)
    return document


def parse(document: Any, key: str | None = ENTRY_ID) -> Any:
    return parse_event(None if key is None else key.encode(), json.dumps(document).encode())


def test_a_tally_event() -> None:
    parsed = parse(event(somethingNew=True))
    assert parsed.id == uuid.UUID(ENTRY_ID)
    assert parsed.kind == "transfer"
    assert parsed.created_at == datetime(2026, 11, 2, 16, 0, 23, 696000, tzinfo=UTC)
    assert [line.amount for line in parsed.lines] == [-1500, 1500]


def test_an_event_from_before_tallys_v7() -> None:
    document = event()
    del document["decisionId"]
    del document["riskCheck"]
    parsed = parse(document)
    assert parsed.decision_id is None
    assert parsed.risk_check == "none"


def test_a_multi_currency_entry_balances_per_currency() -> None:
    lines = [
        {"id": 1, "accountId": ALICE, "amount": -100, "currency": "CAD"},
        {"id": 2, "accountId": BOB, "amount": 100, "currency": "CAD"},
        {"id": 3, "accountId": ALICE, "amount": 7, "currency": "USD"},
        {"id": 4, "accountId": BOB, "amount": -7, "currency": "USD"},
    ]
    assert len(parse(event(kind="entry", lines=lines)).lines) == 4


def without(name: str) -> dict[str, Any]:
    document = event()
    del document[name]
    return document


BAD = {
    "kind missing": (without("kind"), ENTRY_ID),
    "id missing": (without("id"), ENTRY_ID),
    "createdAt missing": (without("createdAt"), ENTRY_ID),
    "lines missing": (without("lines"), ENTRY_ID),
    "id not a UUID": (event(id="x"), "x"),
    "a key that isn't the id": (event(), str(uuid.uuid4())),
    "no key": (event(), None),
    "an unknown kind": (event(kind="refund"), ENTRY_ID),
    "no lines": (event(lines=[]), ENTRY_ID),
    "a zero line": (
        event(lines=[{"id": 1, "accountId": ALICE, "amount": 0, "currency": "CAD"}]),
        ENTRY_ID,
    ),
    "lines that don't sum to zero": (
        event(lines=[{"id": 1, "accountId": ALICE, "amount": 5, "currency": "CAD"}]),
        ENTRY_ID,
    ),
    "a line amount as a string": (
        event(
            lines=[
                {"id": 1, "accountId": ALICE, "amount": "-5", "currency": "CAD"},
                {"id": 2, "accountId": BOB, "amount": "5", "currency": "CAD"},
            ]
        ),
        ENTRY_ID,
    ),
    "createdAt with an offset": (event(createdAt="2026-11-02T16:00:23+00:00"), ENTRY_ID),
    "createdAt with 2 fraction digits": (event(createdAt="2026-11-02T16:00:23.12Z"), ENTRY_ID),
    "decisionId not a UUID": (event(decisionId=5), ENTRY_ID),
    "riskCheck a number": (event(riskCheck=1), ENTRY_ID),
}


@pytest.mark.parametrize(("document", "key"), BAD.values(), ids=BAD.keys())
def test_a_bad_event(document: dict[str, Any], key: str | None) -> None:
    with pytest.raises(BadEventError):
        parse(document, key)


@pytest.mark.parametrize("raw", [b"not json", b"[]", b"\xff"], ids=["not JSON", "array", "bytes"])
def test_a_value_that_is_not_a_json_object(raw: bytes) -> None:
    with pytest.raises(BadEventError):
        parse_event(ENTRY_ID.encode(), raw)
