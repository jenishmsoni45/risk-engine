"""I5, schema half: every decision row stores the rule set's version and hash, the fired rules, a
feature snapshot and the lag, and the schema refuses a row without them (design.md section 2).
Phase 2, criterion 8. The check that the snapshot holds all 31 names comes in Phase 3 (D43).
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
import redis
from psycopg import errors, sql
from psycopg.types.json import Jsonb

from risk.timestamps import epoch_ms, parse_offset_timestamp
from tests.helpers import ROOT, database, decision_request, post_decision, unique

RULES_FILE = ROOT / "risk" / "rules" / "ruleset.yaml"
FROM = uuid.UUID("7c1e9a52-3a0f-4b8e-9d55-0f3c2b1a6e47")


def complete_row() -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "decision_id": uuid.uuid4(),
        "request_id": unique("i5"),
        "from_account_id": FROM,
        "to_account_id": uuid.uuid4(),
        "amount": 1,
        "currency": "CAD",
        "requested_at": now,
        "decided_at": now,
        "decision": "allow",
        "rule_set_version": 1,
        "rule_set_hash": "0" * 64,
        "fired": Jsonb([]),
        "features": Jsonb({"amount": 1}),
        "feature_lag_ms": 0,
    }


def insert(row: dict[str, Any]) -> None:
    """Inserts the row in a transaction that is always rolled back, so nothing is left behind."""
    statement = sql.SQL("insert into decisions ({}) values ({})").format(
        sql.SQL(", ").join(map(sql.Identifier, row)),
        sql.SQL(", ").join(sql.Placeholder() * len(row)),
    )
    with database() as conn, conn.transaction(force_rollback=True):
        conn.execute(statement, tuple(row.values()))


def test_a_complete_row_is_accepted() -> None:
    insert(complete_row())


I5_FIELDS = ["rule_set_version", "rule_set_hash", "fired", "features", "feature_lag_ms"]


@pytest.mark.parametrize("column", I5_FIELDS)
def test_a_row_without_a_field_is_refused(column: str) -> None:
    row = complete_row()
    del row[column]
    with pytest.raises(errors.NotNullViolation):
        insert(row)


@pytest.mark.parametrize("column", I5_FIELDS)
def test_a_row_with_a_null_field_is_refused(column: str) -> None:
    with pytest.raises(errors.NotNullViolation):
        insert({**complete_row(), column: None})


MALFORMED = {
    "version 0": ("rule_set_version", 0),
    "hash too short": ("rule_set_hash", "ab" * 31),
    "hash uppercase": ("rule_set_hash", "A" * 64),
    "hash not hex": ("rule_set_hash", "g" * 64),
    "fired an object": ("fired", Jsonb({})),
    "fired a string": ("fired", Jsonb("velocity_1m")),
    "fired holding a number": ("fired", Jsonb(["velocity_1m", 1])),
    "features an array": ("features", Jsonb([])),
    "features null in JSON": ("features", Jsonb(None)),
    "decision uppercase": ("decision", "ALLOW"),
    "amount zero": ("amount", 0),
    "currency lowercase": ("currency", "cad"),
    "the same account twice": ("to_account_id", FROM),
}


@pytest.mark.parametrize(("column", "value"), MALFORMED.values(), ids=MALFORMED.keys())
def test_a_malformed_field_is_refused(column: str, value: Any) -> None:
    with pytest.raises(errors.CheckViolation):
        insert({**complete_row(), column: value})


def watermark() -> int:
    """The watermark the API reads; a missing one reads as 0 (D43)."""
    raw = redis.Redis(host="127.0.0.1", port=6379, socket_timeout=5).get("wm")
    return 0 if raw is None else int(raw)


def test_a_real_decision_row_holds_everything() -> None:
    body = decision_request(amount=4321)
    before = watermark()
    response = post_decision(body)
    after = watermark()
    assert response.status_code == 200
    with database() as conn:
        row = conn.execute(
            "select decision_id, rule_set_version, rule_set_hash, fired, features, feature_lag_ms "
            "from decisions where request_id = %s",
            (body["requestId"],),
        ).fetchone()
    assert row is not None
    decision_id, version, rule_hash, fired, features, lag = row
    assert str(decision_id) == response.json()["decisionId"]
    assert version == 1
    assert rule_hash == hashlib.sha256(RULES_FILE.read_bytes()).hexdigest()
    assert fired == []
    # Before Phase 3, the amount is the only feature there is (D43).
    assert features == {"amount": 4321}
    # lag = t - wm, with the watermark the API read; it only ever rises (design.md 3.2).
    t = epoch_ms(parse_offset_timestamp(body["requestedAt"]))
    assert t - after <= lag <= t - before
