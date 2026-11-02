"""A consumer killed between its writes and its commit (design.md 3.2 step 5, D40h). Phase 3,
criterion 7: it restarts with features still equal to the oracle's.

The stack's consumer is stopped, so the consumer group is free. The real consumer loop then runs
on this machine (tests/consumer_crash.py) in the same group and dies like a killed process right
after its first batch's Redis writes, before committing them. The stack's consumer starts again,
reads those messages again from the committed offsets, and the features equal the oracle's:
writing them twice changed nothing (I2).
"""

from __future__ import annotations

import os
import subprocess
import sys
import uuid

from risk.features.redis_store import ledger_key, member
from tests import oracle
from tests.consumer_crash import CRASH_EXIT
from tests.feature_state import STACK_DB, Query, client, read, redis_url
from tests.helpers import (
    KAFKA_HOST_BOOTSTRAP,
    ROOT,
    ConsumerOffsets,
    compose,
    database,
    decision_request,
    free_port,
    iso_ms,
    now_ms,
    post_decision,
    postgres_url,
    publish,
    wait_until_healthy,
)
from tests.ledger_events import SECOND_US, Event, entry, transfer


def crash_environment() -> dict[str, str]:
    return {
        **os.environ,
        "KAFKA_BOOTSTRAP": KAFKA_HOST_BOOTSTRAP,
        "REDIS_URL": redis_url(STACK_DB),
        "DATABASE_URL": postgres_url(),
        "CONSUMER_METRICS_PORT": str(free_port()),
    }


def written(event: Event) -> bool:
    """Whether the entry's members are in the stack's Redis."""
    db = client(STACK_DB)
    entry_ = event.oracle()
    nets = {line.account: oracle.net(entry_, line.account) for line in entry_.lines}
    return any(
        db.zscore(ledger_key(account, "out" if net < 0 else "in"), member(abs(net), str(event.id)))
        is not None
        for account, net in nets.items()
        if net
    )


def test_a_consumer_killed_before_its_commit_restarts_with_features_equal_to_the_oracles() -> None:
    sender, receiver, x, y = (uuid.uuid4() for _ in range(4))
    t = now_ms()
    t_us = t * 1000
    events = [transfer(sender, x, 100 + n, t_us - (n + 1) * 7 * SECOND_US) for n in range(12)]
    events += [transfer(x, receiver, 300 + n, t_us - (n + 1) * 11 * SECOND_US) for n in range(12)]
    events += [entry([(sender, -1000), (receiver, 600), (y, 400)], t_us - 90 * SECOND_US)]
    events += [entry([(receiver, -50), (sender, 50)], t_us - 2 * SECOND_US)]

    offsets = ConsumerOffsets()
    compose("stop", "consumer")
    try:
        committed_before = offsets.committed()
        positions = publish([(e.key(), e.value()) for e in events])
        crashed = subprocess.run(
            [sys.executable, "-m", "tests.consumer_crash"],
            cwd=ROOT,
            env=crash_environment(),
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        assert crashed.returncode == CRASH_EXIT, crashed.stdout + crashed.stderr
        # It wrote and never committed.
        assert any(written(e) for e in events), crashed.stdout
        assert offsets.committed() == committed_before
    finally:
        compose("start", "consumer")
        wait_until_healthy("consumer")

    offsets.wait_past(positions, timeout=90)
    offsets.close()
    assert all(written(e) for e in events)

    expected = oracle.features([e.oracle() for e in events], [], sender, receiver, 1, t)
    ((features, _),) = read(STACK_DB, [Query(sender, receiver, 1, t)])
    assert features == expected
    # 8 of the sender's transfers are inside the minute; the receiver got 12 transfers and 600.
    assert (expected["out_count_1m"], expected["dest_in_count_1h"]) == (8, 13)

    # And a decision made now logs the same features.
    body = decision_request(
        fromAccountId=str(sender), toAccountId=str(receiver), amount=1, requestedAt=iso_ms(t)
    )
    assert post_decision(body).status_code == 200
    with database() as conn:
        row = conn.execute(
            "select features from decisions where request_id = %s", (body["requestId"],)
        ).fetchone()
    assert row == (expected,)
