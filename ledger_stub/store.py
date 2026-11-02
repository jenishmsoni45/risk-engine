"""The stub's keys, saved responses and outbox in Postgres (design.md section 2, D40l)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC
from typing import Any

from psycopg import AsyncConnection
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from ledger_stub.transfers import TransferRequest, transfer_entry

CREATED = 201

SELECT_SAVED = """
select request_hash, response_status, response_body
from stub.transfers
where idempotency_key = %s
"""

INSERT_TRANSFER = """
insert into stub.transfers (
    idempotency_key, request_hash, response_status, response_body, entry_id, decision_id,
    risk_check, created_at
) values (%s, %s, %s, %s, %s, %s, %s, %s)
on conflict (idempotency_key) do nothing
returning entry_id
"""

INSERT_OUTBOX = "insert into stub.outbox (entry_id, payload) values (%s, %s)"


@dataclass(frozen=True)
class Saved:
    """A key whose transfer was posted: replayed if the request matches, else a 409."""

    request_hash: str
    status: int
    body: dict[str, Any]


@dataclass(frozen=True)
class Posted:
    entry: dict[str, Any]


async def _saved(conn: AsyncConnection, key: str) -> Saved | None:
    cursor = await conn.execute(SELECT_SAVED, (key,))
    row = await cursor.fetchone()
    return None if row is None else Saved(*row)


async def saved(pool: AsyncConnectionPool, key: str) -> Saved | None:
    """The saved response for a key, read before the decision call and outside any transaction,
    as tally does (tally-integration.md 4.1, step 2)."""
    async with pool.connection() as conn:
        return await _saved(conn, key)


async def post(
    pool: AsyncConnectionPool,
    *,
    key: str,
    request_hash: str,
    request: TransferRequest,
    risk_check: str,
    decision_id: uuid.UUID | None,
) -> Posted | Saved:
    """Posts the transfer: its key with the saved 201 and its outbox row, in one transaction.

    If a concurrent request with the same key committed first, nothing is written and that
    request's saved response comes back instead, to replay or answer 409, as tally's claim does.
    """
    async with pool.connection() as conn, conn.transaction():
        cursor = await conn.execute(
            "select now(), nextval('stub.line_ids'), nextval('stub.line_ids')"
        )
        row = await cursor.fetchone()
        if row is None:
            raise RuntimeError("select now() returned no row")
        created_at, from_line, to_line = row
        entry_id = uuid.uuid4()
        entry = transfer_entry(
            entry_id=entry_id,
            request=request,
            created_at=created_at.astimezone(UTC),
            line_ids=(from_line, to_line),
            risk_check=risk_check,
            decision_id=decision_id,
        )
        cursor = await conn.execute(
            INSERT_TRANSFER,
            (
                key,
                request_hash,
                CREATED,
                Jsonb(entry),
                entry_id,
                decision_id,
                risk_check,
                created_at,
            ),
        )
        if await cursor.fetchone() is None:
            existing = await _saved(conn, key)
            if existing is None:
                raise RuntimeError(f"key {key!r} conflicted but has no saved response")
            return existing
        await conn.execute(INSERT_OUTBOX, (entry_id, Jsonb(entry)))
    return Posted(entry)
