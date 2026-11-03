"""Floods (design.md section 11, adversarial). Phase 5, criterion 5: a flood of every event
repeated, a flood of late events, and a burst of concurrent requests with one requestId leave
features and rows exactly as the oracle and I1 say.

Everything goes through the stack: events through its Kafka and consumer, requests to its API.
"""

from __future__ import annotations

import asyncio
import random
import uuid

import httpx

from risk import settings
from risk.features.redis_store import WATERMARK_KEY, attempt_key, ledger_key
from tests import oracle
from tests.feature_state import STACK_DB, Query, client, read
from tests.helpers import (
    API_URL,
    CONSUMER_METRICS_URL,
    count,
    decision_request,
    iso_ms,
    metric,
    now_ms,
    publish,
    wait_until_consumed,
)
from tests.ledger_events import HOUR_US, MINUTE_US, SECOND_US, entry, transfer

DAY_MS = 24 * 3600 * 1000
CUTOFF_MS = DAY_MS + settings.lateness_margin_ms()  # D40c

# Sizes, stated here as criterion 5 asks.
DISTINCT_EVENTS = 1_000
COPIES = 3
LATE_EVENTS = 2_000
BURST = 200


def test_a_flood_of_every_event_repeated_gives_the_oracles_features() -> None:
    accounts = [uuid.uuid4() for _ in range(10)]
    base = max(now_ms(), int(client(STACK_DB).get(WATERMARK_KEY) or 0))
    rng = random.Random(5)  # noqa: S311 - fixed test data, not a secret
    events = []
    for n in range(DISTINCT_EVENTS):
        sender, receiver = rng.sample(accounts, 2)
        time_us = base * 1000 - rng.randrange(23 * HOUR_US)
        if n % 10 == 0:
            third = rng.choice(accounts)
            events.append(entry([(sender, -300), (receiver, 200), (third, 100)], time_us))
        else:
            events.append(transfer(sender, receiver, rng.randrange(1, 10_000), time_us))
    # Every event three times, all in a shuffled order across partitions.
    deliveries = [e for e in events for _ in range(COPIES)]
    rng.shuffle(deliveries)
    bad_before = count("select count(*) from bad_events")

    wait_until_consumed(publish([(e.key(), e.value()) for e in deliveries]), timeout=120)

    queries = [
        Query(accounts[i], accounts[(i + 1) % 10], 1, t)
        for i in range(10)
        for t in (base + 1, base - 30 * 60_000)
    ]
    expected = [
        oracle.features([e.oracle() for e in events], [], q.sender, q.receiver, 1, q.t_ms)
        for q in queries
    ]
    assert [features for features, _ in read(STACK_DB, queries)] == expected
    # Each entry is one member per account, however often it came: the sets hold no copies.
    db = client(STACK_DB)
    assert sum(db.zcard(ledger_key(a, "out")) for a in accounts) == len(events)
    assert count("select count(*) from bad_events") == bad_before


def test_a_flood_of_late_events_is_dropped_and_counted_and_changes_nothing() -> None:
    db = client(STACK_DB)
    newest_ms = max(now_ms(), int(db.get(WATERMARK_KEY) or 0))
    anchor = transfer(uuid.uuid4(), uuid.uuid4(), 1, newest_ms * 1000)
    wait_until_consumed(publish([(anchor.key(), anchor.value())]))

    sender, receiver = uuid.uuid4(), uuid.uuid4()
    too_old_us = (newest_ms - CUTOFF_MS) * 1000 - MINUTE_US
    late = [
        transfer(sender, receiver, 1 + n, too_old_us - n * SECOND_US) for n in range(LATE_EVENTS)
    ]
    late_before = metric(CONSUMER_METRICS_URL, "risk_consumer_events_total", outcome="late")

    wait_until_consumed(publish([(e.key(), e.value()) for e in late]), timeout=120)

    assert (
        metric(CONSUMER_METRICS_URL, "risk_consumer_events_total", outcome="late")
        == late_before + LATE_EVENTS
    )
    assert db.exists(ledger_key(sender, "out"), ledger_key(receiver, "in")) == 0
    # A late event never moves the watermark.
    assert int(db.get(WATERMARK_KEY) or 0) >= newest_ms
    ((features, _),) = read(STACK_DB, [Query(sender, receiver, 1, newest_ms + 1)])
    assert features == oracle.features([], [], sender, receiver, 1, newest_ms + 1)


def test_a_burst_of_one_request_id_gives_one_row_one_decision_and_no_second_attempt() -> None:
    body = decision_request(requestedAt=iso_ms(now_ms()))

    async def burst() -> list[httpx.Response]:
        limits = httpx.Limits(max_connections=BURST, max_keepalive_connections=BURST)
        async with httpx.AsyncClient(base_url=API_URL, timeout=10, limits=limits) as api:
            return await asyncio.gather(
                *(api.post("/v1/decisions", json=body) for _ in range(BURST))
            )

    responses = asyncio.run(burst())

    # Every answer is the one decision, or a 503 when the API couldn't finish in its 60 ms
    # deadline, which is no decision at all (D22); never a second decision.
    statuses = [r.status_code for r in responses]
    assert set(statuses) <= {200, 503}, statuses
    # On a busy runner every request of the burst may miss the deadline (D51.3, D53), so one
    # more request, alone, must get the decision, new or stored, and it joins the comparison.
    after = httpx.post(f"{API_URL}/v1/decisions", json=body, timeout=10)
    assert after.status_code == 200, after.text
    decided = [r.json() for r in responses if r.status_code == 200] + [after.json()]
    first = decided[0]
    for answer in decided:
        for name in ("decisionId", "decision", "ruleSet", "fired"):
            assert answer[name] == first[name]
    assert count("select count(*) from decisions where request_id = %s", (body["requestId"],)) == 1
    # The replays added no attempt, so there is never a second (D53). There may be none: when
    # the deadline cut off the request whose insert still committed (design.md section 4), it
    # answered 503 without recording one, and the rebuild restores it (3.5). test_attempts.py
    # checks that a new decision answered 200 adds exactly one.
    sender = uuid.UUID(body["fromAccountId"])
    assert client(STACK_DB).zcard(attempt_key(sender)) <= 1
