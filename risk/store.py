"""The decision log in Postgres (design.md section 2). Parameterised SQL only."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

from psycopg import AsyncConnection
from psycopg.types.json import Jsonb


@dataclass(frozen=True)
class StoredDecision:
    decision_id: uuid.UUID
    from_account_id: uuid.UUID
    to_account_id: uuid.UUID
    amount: int
    currency: str
    decision: str
    rule_set_version: int
    fired: list[str]


@dataclass(frozen=True)
class NewDecision:
    decision_id: uuid.UUID
    request_id: str
    from_account_id: uuid.UUID
    to_account_id: uuid.UUID
    amount: int
    currency: str
    requested_at: datetime
    decided_at: datetime
    decision: str
    rule_set_version: int
    rule_set_hash: str
    fired: list[str]
    features: dict[str, int]
    feature_lag_ms: int
    # 2b (D39, D58c): null with shadow off; a failed score is null beside its model's name.
    shadow_score: float | None
    shadow_model: str | None


FIND = """
select decision_id, from_account_id, to_account_id, amount, currency, decision,
       rule_set_version, fired
from decisions
where request_id = %s
"""

INSERT = """
insert into decisions (
    decision_id, request_id, from_account_id, to_account_id, amount, currency, requested_at,
    decided_at, decision, rule_set_version, rule_set_hash, fired, features, feature_lag_ms,
    shadow_score, shadow_model
) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
on conflict (request_id) do nothing
returning decision_id
"""


async def find(conn: AsyncConnection, request_id: str) -> StoredDecision | None:
    cursor = await conn.execute(FIND, (request_id,))
    row = await cursor.fetchone()
    if row is None:
        return None
    return StoredDecision(*row)


async def insert(conn: AsyncConnection, row: NewDecision) -> bool:
    """True when this row was stored; False when another row already has its request_id."""
    cursor = await conn.execute(
        INSERT,
        (
            row.decision_id,
            row.request_id,
            row.from_account_id,
            row.to_account_id,
            row.amount,
            row.currency,
            row.requested_at,
            row.decided_at,
            row.decision,
            row.rule_set_version,
            row.rule_set_hash,
            Jsonb(row.fired),
            Jsonb(row.features),
            row.feature_lag_ms,
            row.shadow_score,
            row.shadow_model,
        ),
    )
    return await cursor.fetchone() is not None
