"""A short history and a run of requests that fire every starter rule (D20) and fill every feature
family, over accounts named inside a namespace, so one stack can run the same scenario twice with
identical features: once per API being compared.

Times are minutes before now, so the events are never late behind the stack's watermark.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any

from tests.helpers import iso_ms, now_ms, unique
from tests.ledger_events import Event, transfer

SECOND_MS = 1000
MINUTE_MS = 60 * SECOND_MS
STARTER_RULE_IDS = frozenset(
    {
        "velocity_1m",
        "attempt_burst_1m",
        "daily_outflow_24h",
        "large_transfer",
        "dest_fan_in_1h",
        "pass_through_1h",
    }
)


@dataclass(frozen=True)
class Scenario:
    namespace: uuid.UUID = field(default_factory=uuid.uuid4)
    base_ms: int = field(default_factory=lambda: now_ms() - 30 * MINUTE_MS)

    def account(self, name: str) -> uuid.UUID:
        return uuid.uuid5(self.namespace, name)

    def _transfer(self, sender: str, receiver: str, amount: int, offset_ms: int) -> Event:
        return transfer(
            self.account(sender),
            self.account(receiver),
            amount,
            (self.base_ms + offset_ms) * 1000,
        )

    def events(self) -> list[Event]:
        """The history, published before any request."""
        return [
            # c1 sends 5 times within the minute before its request: velocity_1m.
            *(self._transfer("c1", "c2", 100, s * SECOND_MS) for s in (10, 20, 30, 40, 50)),
            # d1 receives 300,000 within the hour before it sends 250,000: pass_through_1h.
            self._transfer("d0", "d1", 300_000, 0),
            # e0 receives from 20 senders within the hour: dest_fan_in_1h.
            *(self._transfer(f"e{n}", "e0", 100, n * 30 * SECOND_MS) for n in range(1, 21)),
            # h1 sends 2,700,000 within the day: daily_outflow_24h.
            *(self._transfer("h1", "h2", 900_000, s * SECOND_MS) for s in (0, 1, 2)),
        ]

    def _request(self, sender: str, receiver: str, amount: int, offset_ms: int) -> dict[str, Any]:
        return {
            "requestId": unique("scenario"),
            "fromAccountId": str(self.account(sender)),
            "toAccountId": str(self.account(receiver)),
            "amount": amount,
            "currency": "CAD",
            "requestedAt": iso_ms(self.base_ms + offset_ms),
        }

    def requests(self) -> list[dict[str, Any]]:
        """The requests, in time order; each call gives new requestIds."""
        return [
            self._request("a1", "a2", 2_500, 0),  # nothing fires
            self._request("b1", "b2", 1_000_000, SECOND_MS),  # large_transfer
            self._request("c1", "c3", 100, MINUTE_MS),  # velocity_1m
            self._request("z1", "c1", 500, MINUTE_MS + SECOND_MS),  # the receiver's outflows
            self._request("d1", "d2", 250_000, 2 * MINUTE_MS),  # pass_through_1h
            self._request("f1", "e0", 100, 11 * MINUTE_MS),  # dest_fan_in_1h
            # Ten attempts within a minute, then an eleventh: attempt_burst_1m blocks it.
            *(self._request("g1", "g2", 100, 12 * MINUTE_MS + s * SECOND_MS) for s in range(11)),
            self._request("h1", "h3", 100, 13 * MINUTE_MS),  # daily_outflow_24h
        ]
