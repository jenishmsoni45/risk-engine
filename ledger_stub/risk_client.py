"""The stub's decision call, as tally's RiskClient makes it (tally-integration.md 3.2 to 4.3).

One deadline covers the whole call, from connecting to reading the body, so nothing the decision
API does can make the caller wait longer (I4). Only a 200 whose body keeps the contract is a
decision; everything else is no decision, for which the caller uses the backup answer.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from risk.formats import CANONICAL_UUID
from risk.settings import DECISIONS
from risk.timestamps import format_instant

MAX_REPLY_BYTES = 16 * 1024
# Below the 5 s for which the decision API keeps an idle connection, so the stub drops an idle
# connection first and never sends on one being closed. tally uses the same 4 s (4.3).
KEEP_ALIVE_S = 4.0


@dataclass(frozen=True)
class Decided:
    decision_id: uuid.UUID
    decision: str
    on_timeout: str


@dataclass(frozen=True)
class ApiConflict:
    """The decision API answered 409: the key was decided for a different transfer."""


@dataclass(frozen=True)
class NoDecision:
    reason: str  # timeout, unavailable or invalid, tally's outcome tags


RiskResult = Decided | ApiConflict | NoDecision


def parse_reply(body: bytes) -> Decided | None:
    """A 200 body that keeps the contract (3.2), or None. Unknown fields are ignored."""
    if len(body) > MAX_REPLY_BYTES:
        return None
    try:
        document: Any = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(document, dict):
        return None
    decision_id = document.get("decisionId")
    decision = document.get("decision")
    on_timeout = document.get("onTimeout")
    if not (isinstance(decision_id, str) and CANONICAL_UUID.fullmatch(decision_id)):
        return None
    if not (isinstance(decision, str) and decision in DECISIONS):
        return None
    if not (isinstance(on_timeout, str) and on_timeout in DECISIONS):
        return None
    return Decided(uuid.UUID(decision_id), decision, on_timeout)


class RiskClient:
    def __init__(self, base_url: str, timeout_s: float, on_timeout_default: str) -> None:
        self.timeout_s = timeout_s
        # The onTimeout of the latest valid 200, or the configured default before one (D10).
        self.backup_answer = on_timeout_default
        self._client = httpx.AsyncClient(
            base_url=base_url,
            # A backstop only: the deadline in decide() is the one that ends a call.
            timeout=httpx.Timeout(timeout_s),
            limits=httpx.Limits(keepalive_expiry=KEEP_ALIVE_S),
            follow_redirects=False,
            # tally asks without compression (3.2).
            headers={"accept-encoding": "identity"},
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def decide(
        self,
        request_id: str,
        from_account_id: uuid.UUID,
        to_account_id: uuid.UUID,
        amount: int,
        currency: str,
    ) -> RiskResult:
        body = json.dumps(
            {
                "requestId": request_id,
                "fromAccountId": str(from_account_id),
                "toAccountId": str(to_account_id),
                "amount": amount,
                "currency": currency,
                # tally's clock just before the call (3.1).
                "requestedAt": format_instant(datetime.now(UTC)),
            }
        ).encode()
        started = time.monotonic()
        result: RiskResult
        try:
            async with asyncio.timeout(self.timeout_s):
                result = await self._ask(body)
        except TimeoutError:
            result = NoDecision("timeout")
        except (httpx.HTTPError, OSError):
            # Refused, reset or broken; or httpx's own timeout, which the clock tells apart.
            timed_out = time.monotonic() - started >= self.timeout_s
            result = NoDecision("timeout" if timed_out else "unavailable")
        if isinstance(result, Decided):
            self.backup_answer = result.on_timeout
        return result

    async def _ask(self, body: bytes) -> RiskResult:
        async with self._client.stream(
            "POST", "/v1/decisions", content=body, headers={"content-type": "application/json"}
        ) as response:
            # No body is read on a 409 (D13).
            if response.status_code == 409:
                return ApiConflict()
            if response.status_code != 200:
                return NoDecision("unavailable")
            data = bytearray()
            async for chunk in response.aiter_raw():
                data.extend(chunk)
                if len(data) > MAX_REPLY_BYTES:
                    return NoDecision("invalid")
        decided = parse_reply(bytes(data))
        return decided if decided is not None else NoDecision("invalid")
