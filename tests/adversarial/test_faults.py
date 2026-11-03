"""The failure table (design.md section 7). Phase 5, criterion 3: each row, for the decision API,
Redis, Postgres and Kafka paused and the consumer killed, ends in the behaviour the table states.

Every test runs against the Compose stack, with `docker compose pause` (or `kill`) on one service
and the stub container as the caller. Timings use D40m's margin of 100 ms. The table's last row
for Redis, "Redis data lost", is tests/adversarial/test_rebuild.py.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

import httpx

from risk.features.redis_store import WATERMARK_KEY, ledger_key, member
from tests import oracle
from tests.feature_state import STACK_DB, Query, client, read
from tests.helpers import (
    API_URL,
    CONSUMER_METRICS_URL,
    STUB_URL,
    TIMING_MARGIN_S,
    ConsumerOffsets,
    compose,
    count,
    database,
    decision_request,
    decision_rows,
    metric,
    now_ms,
    paused,
    publish,
    unique,
    wait_until_healthy,
)
from tests.ledger_events import SECOND_US, Event, transfer

API_DEADLINE_S = 0.060  # D40d
CALLER_TIMEOUT_S = 0.100  # D4
STUB_DATABASE_STEP_S = 5.0  # D48.3
RISK_UNAVAILABLE = "/problems/risk-unavailable"


def transfer_body(sender: uuid.UUID, receiver: uuid.UUID, amount: int = 100) -> dict[str, Any]:
    return {
        "fromAccountId": str(sender),
        "toAccountId": str(receiver),
        "amount": amount,
        "currency": "CAD",
    }


def timed_transfer(
    stub: httpx.Client, key: str, body: dict[str, Any]
) -> tuple[httpx.Response, float]:
    started = time.monotonic()
    response = stub.post("/transfers", json=body, headers={"Idempotency-Key": key})
    return response, time.monotonic() - started


def timed_decision(api: httpx.Client, body: dict[str, Any]) -> tuple[httpx.Response, float]:
    started = time.monotonic()
    response = api.post("/v1/decisions", json=body)
    return response, time.monotonic() - started


def saved_keys(key: str) -> int:
    return count("select count(*) from stub.transfers where idempotency_key = %s", (key,))


def wait_for(condition: Any, what: str, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, f"{what} within {timeout} s"
        time.sleep(0.1)


def lag_and_features(decision_id: str) -> tuple[int, dict[str, int]]:
    with database() as conn:
        row = conn.execute(
            "select feature_lag_ms, features from decisions where decision_id = %s",
            (decision_id,),
        ).fetchone()
    assert row is not None, decision_id
    return row[0], row[1]


def applied(event: Event, account: uuid.UUID, amount: int) -> bool:
    return (
        client(STACK_DB).zscore(ledger_key(account, "out"), member(amount, str(event.id)))
        is not None
    )


# --- Decision API down, paused or slow --------------------------------------------------------


def test_api_paused_the_caller_gives_the_backup_answer_within_its_timeout() -> None:
    backups_before = metric(STUB_URL, "stub_backup_answers_total", answer="hold")
    key = unique("fault-api")
    body = transfer_body(uuid.uuid4(), uuid.uuid4())
    with httpx.Client(base_url=STUB_URL, timeout=5) as stub:
        # An open connection, so the time measured is the stub's and not a TCP handshake.
        assert stub.get("/metrics").status_code == 200
        with paused("api"):
            response, elapsed = timed_transfer(stub, key, body)

        assert elapsed <= CALLER_TIMEOUT_S + TIMING_MARGIN_S, f"{elapsed * 1000:.0f} ms"
        # The starter rule set's backup answer is hold (D5), so the customer is told to retry (D6).
        assert response.status_code == 503
        assert response.json()["type"] == RISK_UNAVAILABLE
        assert metric(STUB_URL, "stub_backup_answers_total", answer="hold") == backups_before + 1
        assert saved_keys(key) == 0

        # Once the API is back, the same key gets a real decision and posts.
        retried = stub.post("/transfers", json=body, headers={"Idempotency-Key": key})
    assert retried.status_code == 201
    assert retried.json()["riskCheck"] == "decided"


# --- Redis down or slow -----------------------------------------------------------------------


def test_redis_paused_the_api_answers_503_in_time_and_the_consumer_loses_nothing() -> None:
    sender, receiver = uuid.uuid4(), uuid.uuid4()
    t_ms = max(now_ms(), int(client(STACK_DB).get(WATERMARK_KEY) or 0))
    event = transfer(sender, receiver, 500, t_ms * 1000)
    decision = decision_request()
    retries_before = metric(CONSUMER_METRICS_URL, "risk_consumer_retries_total", dependency="redis")
    offsets = ConsumerOffsets()
    with (
        httpx.Client(base_url=API_URL, timeout=5) as api,
        httpx.Client(base_url=STUB_URL, timeout=5) as stub,
    ):
        assert api.get("/metrics").status_code == 200
        assert stub.get("/metrics").status_code == 200
        with paused("redis"):
            # The API: 503 within its deadline, nothing stored.
            response, elapsed = timed_decision(api, decision)
            assert response.status_code == 503
            assert elapsed <= API_DEADLINE_S + TIMING_MARGIN_S, f"{elapsed * 1000:.0f} ms"

            # The caller: the API's 503 is no decision, so the backup answer, at once.
            key = unique("fault-redis")
            answer, elapsed = timed_transfer(stub, key, transfer_body(sender, receiver))
            assert answer.status_code == 503
            assert answer.json()["type"] == RISK_UNAVAILABLE
            assert elapsed <= CALLER_TIMEOUT_S + TIMING_MARGIN_S, f"{elapsed * 1000:.0f} ms"

            # The consumer: its write fails, so it retries and doesn't commit past the event.
            positions = publish([(event.key(), event.value())])
            wait_for(
                lambda: (
                    metric(CONSUMER_METRICS_URL, "risk_consumer_retries_total", dependency="redis")
                    > retries_before
                ),
                "the consumer retried",
            )
            ((partition, offset),) = positions
            assert offsets.committed().get(partition, -1) <= offset

    # Once Redis is back the event is applied and committed: nothing was lost.
    offsets.wait_past(positions, timeout=60)
    offsets.close()
    assert applied(event, sender, 500)
    assert decision_rows(decision["requestId"]) == []
    assert saved_keys(key) == 0


# --- Postgres down ----------------------------------------------------------------------------


def test_postgres_paused_api_and_stub_answer_503_and_only_a_bad_event_waits() -> None:
    sender, receiver = uuid.uuid4(), uuid.uuid4()
    t_ms = max(now_ms(), int(client(STACK_DB).get(WATERMARK_KEY) or 0))
    good = transfer(sender, receiver, 700, t_ms * 1000)
    bad_value = f'{{"bad": "{uuid.uuid4()}"}}'.encode()
    decision = decision_request()
    offsets = ConsumerOffsets()
    with (
        httpx.Client(base_url=API_URL, timeout=5) as api,
        httpx.Client(base_url=STUB_URL, timeout=15) as stub,
    ):
        assert api.get("/metrics").status_code == 200
        assert stub.get("/metrics").status_code == 200
        with paused("postgres"):
            response, elapsed = timed_decision(api, decision)
            assert response.status_code == 503
            assert elapsed <= API_DEADLINE_S + TIMING_MARGIN_S, f"{elapsed * 1000:.0f} ms"

            # The stub answers 503 for its own database, as tally does, within its bound.
            key = unique("fault-postgres")
            answer, elapsed = timed_transfer(stub, key, transfer_body(sender, receiver))
            assert answer.status_code == 503
            assert answer.json()["type"] != RISK_UNAVAILABLE
            assert "database" in answer.json()["detail"]
            assert elapsed <= STUB_DATABASE_STEP_S + TIMING_MARGIN_S, f"{elapsed:.1f} s"

            # The consumer keeps applying valid events; a bad event, which must be stored in
            # Postgres, waits on its own partition.
            ((bad_partition, bad_offset),) = publish([(b"bad", bad_value)], partition=0)
            good_positions = publish([(good.key(), good.value())], partition=1)
            offsets.wait_past(good_positions, timeout=30)
            assert applied(good, sender, 700)
            assert offsets.committed().get(bad_partition, -1) <= bad_offset

    offsets.wait_past([(bad_partition, bad_offset)], timeout=60)
    offsets.close()
    with database() as conn:
        stored = conn.execute(
            "select kafka_partition, kafka_offset from bad_events where message_value = %s",
            (bad_value,),
        ).fetchall()
    assert stored == [(bad_partition, bad_offset)]
    assert decision_rows(decision["requestId"]) == []
    assert saved_keys(key) == 0


# --- Kafka down -------------------------------------------------------------------------------


def test_kafka_paused_the_outbox_grows_and_lag_grows_then_both_recover() -> None:
    sender, receiver = uuid.uuid4(), uuid.uuid4()
    entries: list[dict[str, Any]] = []
    with httpx.Client(base_url=STUB_URL, timeout=5) as stub:
        assert stub.get("/metrics").status_code == 200
        with paused("kafka"):
            for _ in range(3):
                response = stub.post(
                    "/transfers",
                    json=transfer_body(sender, receiver),
                    headers={"Idempotency-Key": unique("fault-kafka")},
                )
                # Decisions and postings go on without Kafka.
                assert response.status_code == 201, response.text
                entries.append(response.json())
                time.sleep(1.5)
            ids = [entry["id"] for entry in entries]
            # The outbox holds every posting until Kafka is back.
            assert (
                count(
                    "select count(*) from stub.outbox "
                    "where entry_id = any(%s::uuid[]) and published_at is null",
                    (ids,),
                )
                == 3
            )
            during = [lag_and_features(entry["decisionId"]) for entry in entries]

    # Lag grows while nothing is applied, and the features go stale: the third decision didn't
    # see the sender's first two transfers.
    lags = [lag for lag, _ in during]
    assert lags[0] < lags[1] < lags[2], lags
    assert during[2][1]["out_count_1m"] == 0
    # Attempts are recorded by the API itself, so they stay fresh whatever the lag (D23).
    assert during[2][1]["attempt_count_1m"] == 2

    # After recovery the outbox drains and the consumer applies the postings.
    wait_for(
        lambda: (
            count(
                "select count(*) from stub.outbox "
                "where entry_id = any(%s::uuid[]) and published_at is null",
                (ids,),
            )
            == 0
        ),
        "the outbox drained",
    )
    wait_for(
        lambda: all(
            client(STACK_DB).zscore(ledger_key(sender, "out"), member(100, entry_id)) is not None
            for entry_id in ids
        ),
        "the consumer applied the postings",
    )

    # A posting made now is applied within the relay's interval, and the lag falls with it.
    with httpx.Client(base_url=STUB_URL, timeout=5) as stub:
        fresh = stub.post(
            "/transfers",
            json=transfer_body(sender, receiver),
            headers={"Idempotency-Key": unique("fault-kafka")},
        ).json()
        wait_for(
            lambda: (
                client(STACK_DB).zscore(ledger_key(sender, "out"), member(100, fresh["id"]))
                is not None
            ),
            "the consumer applied the fresh posting",
        )
        after = stub.post(
            "/transfers",
            json=transfer_body(sender, receiver),
            headers={"Idempotency-Key": unique("fault-kafka")},
        ).json()
    lag_after, features_after = lag_and_features(after["decisionId"])
    assert lag_after < lags[2], (lag_after, lags)
    assert features_after["out_count_1h"] == 4


# --- Consumer killed mid-batch ----------------------------------------------------------------

FLOOD = 10_000


def test_consumer_killed_mid_stream_restarts_from_its_offsets_with_features_unchanged() -> None:
    accounts = [uuid.uuid4() for _ in range(8)]
    base = max(now_ms(), int(client(STACK_DB).get(WATERMARK_KEY) or 0))
    # n and 3n + 1 never agree modulo 8, so no transfer is to its own sender.
    events = [
        transfer(
            accounts[n % 8],
            accounts[(n * 3 + 1) % 8],
            1 + n,
            base * 1000 - (n % 3600) * SECOND_US,
        )
        for n in range(FLOOD)
    ]
    offsets = ConsumerOffsets()
    before = offsets.committed()
    positions = publish([(e.key(), e.value()) for e in events])
    last: dict[int, int] = {}
    for partition, offset in positions:
        last[partition] = max(last.get(partition, -1), offset)

    # Killed once it has committed part of the flood: some messages applied and committed, some
    # applied and not committed, some not read.
    wait_for(
        lambda: any(offsets.committed().get(p, -1) > before.get(p, -1) for p in last),
        "the consumer started on the flood",
        timeout=30,
    )
    compose("kill", "consumer")
    at_kill = offsets.committed()
    assert not all(at_kill.get(p, -1) > o for p, o in last.items()), "the flood ended first"
    compose("start", "consumer")
    wait_until_healthy("consumer")

    offsets.wait_past(positions, timeout=120)
    offsets.close()
    queries = [
        Query(accounts[s], accounts[(s + 1) % 8], 1, t)
        for s in range(0, 8, 3)
        for t in (base + 1, base - 30 * 60_000)
    ]
    expected = [
        oracle.features([e.oracle() for e in events], [], q.sender, q.receiver, 1, q.t_ms)
        for q in queries
    ]
    assert [features for features, _ in read(STACK_DB, queries)] == expected
    assert expected[0]["out_count_1h"] > 0
