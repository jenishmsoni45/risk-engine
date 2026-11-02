"""The stub's parts without a network: tally's transfer validation and request hash, the entry it
posts, and which decision API replies count as a decision (tally-integration.md 3.2)."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from ledger_stub.risk_client import MAX_REPLY_BYTES, Decided, parse_reply
from ledger_stub.transfers import (
    ENTRY_FIELDS,
    LINE_FIELDS,
    InvalidTransferError,
    TransferRequest,
    UnreadableTransferError,
    in_record_order,
    parse_transfer,
    transfer_entry,
)

FROM = "7c1e9a52-3a0f-4b8e-9d55-0f3c2b1a6e47"
TO = "0b6f3d1e-2c4a-4f5b-8e7d-9a1b2c3d4e5f"
DECISION_ID = "5d1c0b2a-9e8f-4a7b-8c6d-5e4f3a2b1c0d"


def transfer(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "fromAccountId": FROM,
        "toAccountId": TO,
        "amount": 500,
        "currency": "CAD",
    }
    body.update(overrides)
    return body


def parse(body: dict[str, Any], key: str | None = "key-1") -> TransferRequest:
    return parse_transfer(key, json.dumps(body).encode())[1]


# --- Validation, as tally's TransferRequest ------------------------------------------------


def test_a_valid_transfer() -> None:
    request = parse(transfer(description="rent", effectiveAt="2026-01-01T00:00:00+01:00"))
    assert request.amount == 500
    assert request.description == "rent"
    assert request.effective_at == datetime(2025, 12, 31, 23, 0, tzinfo=UTC)


INVALID = {
    "no key": (transfer(), None),
    "a key with a space": (transfer(), "a b"),
    "a key over 255": (transfer(), "k" * 256),
    "fromAccountId missing": ({k: v for k, v in transfer().items() if k != "fromAccountId"}, "k"),
    "toAccountId not a UUID": (transfer(toAccountId="x"), "k"),
    "the same account twice": (transfer(toAccountId=FROM), "k"),
    "amount zero": (transfer(amount=0), "k"),
    "amount a float": (transfer(amount=5.0), "k"),
    "amount too large": (transfer(amount=2**63), "k"),
    "currency blank": (transfer(currency="  "), "k"),
    "currency lowercase": (transfer(currency="cad"), "k"),
    "description 501 characters": (transfer(description="d" * 501), "k"),
    # Java counts a character outside the BMP as two.
    "description 251 emoji": (transfer(description="\U0001f600" * 251), "k"),
    "effectiveAt not a timestamp": (transfer(effectiveAt="yesterday"), "k"),
}


@pytest.mark.parametrize(("body", "key"), INVALID.values(), ids=INVALID.keys())
def test_invalid_transfer(body: dict[str, Any], key: str | None) -> None:
    with pytest.raises(InvalidTransferError):
        parse(body, key)


def test_500_characters_is_the_limit() -> None:
    parse(transfer(description="d" * 500))
    parse(transfer(description="\U0001f600" * 250))


@pytest.mark.parametrize("raw", [b"", b"[]", b"nope"])
def test_an_unreadable_body(raw: bytes) -> None:
    with pytest.raises(UnreadableTransferError):
        parse_transfer("k", raw)


# --- The request hash ----------------------------------------------------------------------


def test_the_hash_ignores_how_the_request_was_spelled() -> None:
    base = parse(transfer(effectiveAt="2026-01-01T00:00:00Z")).request_hash()
    reordered = dict(reversed(list(transfer(effectiveAt="2026-01-01T00:00:00Z").items())))
    assert parse(reordered).request_hash() == base
    assert parse(transfer(effectiveAt="2026-01-01T01:00:00.000+01:00")).request_hash() == base
    assert parse(transfer(description=None)).request_hash() == parse(transfer()).request_hash()
    assert (
        parse(transfer(fromAccountId=FROM.upper())).request_hash()
        == parse(transfer()).request_hash()
    )


@pytest.mark.parametrize(
    "change",
    [
        {"fromAccountId": str(uuid.uuid4())},
        {"toAccountId": str(uuid.uuid4())},
        {"amount": 501},
        {"currency": "USD"},
        {"description": "rent"},
        {"effectiveAt": "2026-01-01T00:00:00Z"},
    ],
    ids=str,
)
def test_the_hash_changes_with_the_request(change: dict[str, Any]) -> None:
    assert parse(transfer(**change)).request_hash() != parse(transfer()).request_hash()


# --- The entry -----------------------------------------------------------------------------


def test_the_entry_is_tallys_transfer() -> None:
    created = datetime(2026, 11, 20, 15, 4, 5, 123456, tzinfo=UTC)
    entry_id = uuid.uuid4()
    entry = transfer_entry(
        entry_id=entry_id,
        request=parse(transfer(fromAccountId=FROM.upper())),
        created_at=created,
        line_ids=(7, 8),
        risk_check="decided",
        decision_id=uuid.UUID(DECISION_ID),
    )
    assert tuple(entry) == ENTRY_FIELDS
    assert entry == {
        "id": str(entry_id),
        "kind": "transfer",
        "reversesId": None,
        "description": None,
        # A missing effectiveAt is the posting time, as in tally.
        "effectiveAt": "2026-11-20T15:04:05.123456Z",
        "createdAt": "2026-11-20T15:04:05.123456Z",
        "decisionId": DECISION_ID,
        "riskCheck": "decided",
        "lines": [
            {"id": 7, "accountId": FROM, "amount": -500, "currency": "CAD"},
            {"id": 8, "accountId": TO, "amount": 500, "currency": "CAD"},
        ],
    }
    for line in entry["lines"]:
        assert tuple(line) == LINE_FIELDS


def test_a_saved_entry_replays_in_record_order() -> None:
    entry = transfer_entry(
        entry_id=uuid.uuid4(),
        request=parse(transfer()),
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        line_ids=(1, 2),
        risk_check="fallback",
        decision_id=None,
    )
    # jsonb gives keys back in its own order.
    shuffled = dict(sorted(entry.items()))
    shuffled["lines"] = [dict(sorted(line.items())) for line in entry["lines"]]
    replayed = in_record_order(shuffled)
    assert replayed == entry
    assert tuple(replayed) == ENTRY_FIELDS
    assert [tuple(line) for line in replayed["lines"]] == [LINE_FIELDS, LINE_FIELDS]


# --- Replies that count as a decision ------------------------------------------------------


def reply(**overrides: Any) -> bytes:
    body: dict[str, Any] = {
        "decisionId": DECISION_ID,
        "decision": "hold",
        "ruleSet": "v1",
        "fired": ["velocity_1m"],
        "onTimeout": "allow",
    }
    body.update(overrides)
    return json.dumps(body).encode()


def test_a_valid_reply() -> None:
    assert parse_reply(reply()) == Decided(uuid.UUID(DECISION_ID), "hold", "allow")


@pytest.mark.parametrize(
    "body",
    [
        reply(somethingNew=[1, 2, 3]),
        reply(decisionId=DECISION_ID.upper()),
        reply(ruleSet=None, fired=None),
    ],
    ids=["unknown fields", "uppercase UUID", "ruleSet and fired unread"],
)
def test_accepted_replies(body: bytes) -> None:
    assert parse_reply(body) is not None


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b"[]",
        b"null",
        reply(decisionId="1-2-3-4-5"),
        reply(decisionId=None),
        reply(decisionId=7),
        reply(decision="ALLOW"),
        reply(decision="maybe"),
        reply(decision=None),
        reply(onTimeout="Hold"),
        reply(onTimeout=1),
        reply(fired=["x" * MAX_REPLY_BYTES]),
    ],
    ids=[
        "not JSON",
        "an array",
        "null",
        "short UUID",
        "no decisionId",
        "numeric decisionId",
        "uppercase decision",
        "unknown decision",
        "no decision",
        "capitalised onTimeout",
        "numeric onTimeout",
        "over 16 KiB",
    ],
)
def test_replies_that_are_no_decision(body: bytes) -> None:
    assert parse_reply(body) is None
