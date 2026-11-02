"""The decision request's validation (design.md 1.1): every rule in the table, both ways.

Phase 2, criterion 2 (with tests/integration/test_decision_api.py::test_bad_requests).
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from risk.decisions import InvalidRequestError, parse_request

FROM = "7c1e9a52-3a0f-4b8e-9d55-0f3c2b1a6e47"
TO = "0b6f3d1e-2c4a-4f5b-8e7d-9a1b2c3d4e5f"
MAX = 2**63 - 1


def valid(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "requestId": "key-1",
        "fromAccountId": FROM,
        "toAccountId": TO,
        "amount": 12500,
        "currency": "CAD",
        "requestedAt": "2026-11-20T15:04:05.123Z",
    }
    body.update(overrides)
    return body


def without(name: str) -> dict[str, Any]:
    body = valid()
    del body[name]
    return body


def parse(body: Any) -> Any:
    return parse_request(json.dumps(body).encode())


def test_a_valid_request() -> None:
    request = parse(valid())
    assert request.request_id == "key-1"
    assert request.from_account_id == uuid.UUID(FROM)
    assert request.to_account_id == uuid.UUID(TO)
    assert request.amount == 12500
    assert request.currency == "CAD"
    assert request.requested_at == datetime(2026, 11, 20, 15, 4, 5, 123000, tzinfo=UTC)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2026-11-20T15:04:05Z", datetime(2026, 11, 20, 15, 4, 5, tzinfo=UTC)),
        ("2026-11-20T15:04:05.1Z", datetime(2026, 11, 20, 15, 4, 5, 100000, tzinfo=UTC)),
        # Truncated to the millisecond, never rounded up.
        ("2026-11-20T15:04:05.123999Z", datetime(2026, 11, 20, 15, 4, 5, 123000, tzinfo=UTC)),
        ("2026-11-20T15:04:05.123456789Z", datetime(2026, 11, 20, 15, 4, 5, 123000, tzinfo=UTC)),
        # Converted to UTC.
        ("2026-11-20T16:04:05.123+01:00", datetime(2026, 11, 20, 15, 4, 5, 123000, tzinfo=UTC)),
        ("2026-11-20T00:30:00-05:30", datetime(2026, 11, 20, 6, 0, 0, tzinfo=UTC)),
    ],
)
def test_requested_at_becomes_utc_milliseconds(text: str, expected: datetime) -> None:
    assert parse(valid(requestedAt=text)).requested_at == expected


@pytest.mark.parametrize(
    "overrides",
    [
        {"requestId": "!"},
        {"requestId": "~" * 255},
        {"requestId": "".join(chr(c) for c in range(0x21, 0x7F))},
        {"amount": 1},
        {"amount": MAX},
        {"currency": "XYZ"},
        # Hex digits are case-insensitive in a UUID; the value is the same account.
        {"fromAccountId": FROM.upper()},
        {"unknownField": {"nested": True}},
    ],
    ids=str,
)
def test_accepted_edges(overrides: dict[str, Any]) -> None:
    parse(valid(**overrides))


INVALID: dict[str, Any] = {
    "requestId missing": without("requestId"),
    "requestId null": valid(requestId=None),
    "requestId empty": valid(requestId=""),
    "requestId over 255": valid(requestId="k" * 256),
    "requestId with a space": valid(requestId="a b"),
    "requestId with a tab": valid(requestId="a\tb"),
    "requestId not ASCII": valid(requestId="clé"),
    "requestId a number": valid(requestId=5),
    "fromAccountId missing": without("fromAccountId"),
    "fromAccountId not a UUID": valid(fromAccountId="not-a-uuid"),
    "fromAccountId without hyphens": valid(fromAccountId=FROM.replace("-", "")),
    "fromAccountId in braces": valid(fromAccountId="{" + FROM + "}"),
    "fromAccountId a number": valid(fromAccountId=7),
    "toAccountId missing": without("toAccountId"),
    "toAccountId short form": valid(toAccountId="1-2-3-4-5"),
    "same account": valid(toAccountId=FROM),
    "same account, other case": valid(toAccountId=FROM.upper()),
    "amount missing": without("amount"),
    "amount zero": valid(amount=0),
    "amount negative": valid(amount=-1),
    "amount over 2^63 - 1": valid(amount=MAX + 1),
    "amount a float": valid(amount=1.5),
    "amount a whole float": valid(amount=100.0),
    "amount a string": valid(amount="100"),
    "amount true": valid(amount=True),
    "currency missing": without("currency"),
    "currency lowercase": valid(currency="cad"),
    "currency two letters": valid(currency="CA"),
    "currency four letters": valid(currency="CADX"),
    "currency a number": valid(currency=124),
    "requestedAt missing": without("requestedAt"),
    "requestedAt without an offset": valid(requestedAt="2026-11-20T15:04:05.123"),
    "requestedAt a date only": valid(requestedAt="2026-11-20"),
    "requestedAt lowercase z": valid(requestedAt="2026-11-20T15:04:05z"),
    "requestedAt not a date": valid(requestedAt="2026-02-30T15:04:05Z"),
    "requestedAt offset of 24 h": valid(requestedAt="2026-11-20T15:04:05+24:00"),
    "requestedAt over 9 fraction digits": valid(requestedAt="2026-11-20T15:04:05.1234567890Z"),
    "requestedAt epoch seconds": valid(requestedAt=1795183445),
}


@pytest.mark.parametrize("body", INVALID.values(), ids=INVALID.keys())
def test_invalid_field(body: dict[str, Any]) -> None:
    with pytest.raises(InvalidRequestError):
        parse(body)


@pytest.mark.parametrize(
    "raw",
    [b"", b"not json", b"[]", b'"a string"', b"null", b"{", b"\xff\xfe"],
    ids=["empty", "not JSON", "an array", "a string", "null", "cut short", "not UTF-8"],
)
def test_a_body_that_is_not_a_json_object(raw: bytes) -> None:
    with pytest.raises(InvalidRequestError, match="body"):
        parse_request(raw)


def test_every_broken_field_is_named() -> None:
    with pytest.raises(InvalidRequestError) as caught:
        parse({"amount": 0, "currency": "cad"})
    assert {e.field for e in caught.value.errors} == {
        "requestId",
        "fromAccountId",
        "toAccountId",
        "amount",
        "currency",
        "requestedAt",
    }
