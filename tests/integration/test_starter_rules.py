"""Each starter rule fires end to end (D20). Phase 4, criterion 4: from events published to
ledger.entries and requests to the API, each of D20's six rules fires at least once.

Every test uses new accounts, so nothing else in the stack touches their windows. Events go through
the stack's Kafka and consumer, and each test waits until the consumer has committed past them
(D40h) before it asks. The decision time is at or after the watermark, so no event placed before
it is late (D15). Each answer is checked against its stored row too.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import Any

from risk.features.redis_store import WATERMARK_KEY
from tests.feature_state import STACK_DB, client
from tests.helpers import (
    database,
    decision_request,
    iso_ms,
    now_ms,
    post_decision,
    publish,
    wait_until_consumed,
)
from tests.ledger_events import HOUR_US, MINUTE_US, SECOND_US, Event, transfer

SMALL = 100  # an amount no starter rule fires on by itself


def decision_time_us() -> int:
    """A decision time at or after the watermark, in microseconds."""
    raw = client(STACK_DB).get(WATERMARK_KEY)
    return max(now_ms(), 0 if raw is None else int(raw)) * 1000


def published(events: Sequence[Event]) -> None:
    """Publishes the events to ledger.entries and waits until the consumer has applied them."""
    wait_until_consumed(publish([(e.key(), e.value()) for e in events]))


def ask(sender: uuid.UUID, amount: int, t_us: int, receiver: uuid.UUID | None = None) -> Any:
    """The 200 body for a new request from sender at t, after checking its stored row."""
    body = decision_request(
        fromAccountId=str(sender),
        toAccountId=str(receiver or uuid.uuid4()),
        amount=amount,
        requestedAt=iso_ms(t_us // 1000),
    )
    response = post_decision(body)
    assert response.status_code == 200, response.text
    answer = response.json()
    with database() as conn:
        row = conn.execute(
            "select decision, fired, rule_set_version from decisions where request_id = %s",
            (body["requestId"],),
        ).fetchone()
    assert row == (answer["decision"], answer["fired"], 1)
    return answer


def decided(answer: Any) -> tuple[str, list[str]]:
    return answer["decision"], answer["fired"]


def test_velocity_1m() -> None:
    """5 transfers out in the last minute: hold."""
    sender = uuid.uuid4()
    t = decision_time_us()
    published([transfer(sender, uuid.uuid4(), SMALL, t - n * 10 * SECOND_US) for n in range(1, 6)])
    assert decided(ask(sender, SMALL, t)) == ("hold", ["velocity_1m"])


def test_attempt_burst_1m() -> None:
    """10 requests in the last minute, so the 11th is blocked (design.md 1.4)."""
    sender = uuid.uuid4()
    t = decision_time_us()
    for n in range(10):
        assert decided(ask(sender, SMALL, t - (50 - 5 * n) * SECOND_US)) == ("allow", [])
    assert decided(ask(sender, SMALL, t)) == ("block", ["attempt_burst_1m"])


def test_daily_outflow_24h() -> None:
    """2,500,000 sent in the last 24 h, hours apart so no shorter window fills: hold."""
    sender = uuid.uuid4()
    t = decision_time_us()
    published([transfer(sender, uuid.uuid4(), 1_250_000, t - h * HOUR_US) for h in (20, 2)])
    assert decided(ask(sender, SMALL, t)) == ("hold", ["daily_outflow_24h"])


def test_large_transfer() -> None:
    """An amount of 1,000,000: hold, with no history at all."""
    assert decided(ask(uuid.uuid4(), 1_000_000, decision_time_us())) == ("hold", ["large_transfer"])


def test_dest_fan_in_1h() -> None:
    """The receiver got 20 transfers, from 20 senders, in the last hour: hold."""
    receiver = uuid.uuid4()
    t = decision_time_us()
    published(
        [transfer(uuid.uuid4(), receiver, SMALL, t - (59 - n) * MINUTE_US) for n in range(20)]
    )
    assert decided(ask(uuid.uuid4(), SMALL, t, receiver)) == ("hold", ["dest_fan_in_1h"])


def test_pass_through_1h() -> None:
    """200,000 received in the last hour, then 200,000 sent on: hold."""
    sender = uuid.uuid4()
    t = decision_time_us()
    published([transfer(uuid.uuid4(), sender, 200_000, t - 30 * MINUTE_US)])
    assert decided(ask(sender, 200_000, t)) == ("hold", ["pass_through_1h"])


def test_a_hold_and_a_block_together() -> None:
    """5 transfers out, then 10 requests held by velocity_1m, all in one minute: the 11th request
    fires both rules and is blocked, with both listed in file order."""
    sender = uuid.uuid4()
    t = decision_time_us()
    published([transfer(sender, uuid.uuid4(), SMALL, t - (59 - n) * SECOND_US) for n in range(5)])
    for n in range(10):
        assert decided(ask(sender, SMALL, t - (50 - 5 * n) * SECOND_US)) == (
            "hold",
            ["velocity_1m"],
        )
    assert decided(ask(sender, SMALL, t)) == ("block", ["velocity_1m", "attempt_burst_1m"])
