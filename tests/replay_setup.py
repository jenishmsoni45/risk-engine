"""The stack made ready for a replay (D52b), and generated PaySim transfers to replay (D37).

The replay waits for the watermark to reach each transfer's time, so the watermark must start
behind the first one. On the normal stack it follows the wall clock, ahead of D51.2's January, so
the tests that replay wait until the consumer has applied every event already on the topic, then
empty the stack's Redis, as tests/adversarial/test_rebuild.py does. Each replay maps PaySim names
in a new namespace, so its keys and accounts never meet an earlier run's rows in Postgres.
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path

from ml.generate import generate, write_csv
from ml.paysim import Transfer, load
from tests.feature_state import STACK_DB, client
from tests.helpers import count, topic_ends, wait_until_consumed


def outbox_drained(timeout: float = 30) -> None:
    """Waits until every posted transfer has been published. A stub run in a test process may
    stop with rows left in the shared outbox, which the stack's stub then publishes."""
    deadline = time.monotonic() + timeout
    while count("select count(*) from stub.outbox where published_at is null"):
        assert time.monotonic() < deadline, f"the outbox didn't drain within {timeout} s"
        time.sleep(0.05)


def fresh_watermark() -> None:
    """The stack's Redis emptied once every posted transfer has been published and the consumer
    has applied everything on ledger.entries, so no earlier event can raise the watermark
    afterwards."""
    outbox_drained()
    ends = topic_ends()
    wait_until_consumed([(p, end - 1) for p, end in ends.items() if end > 0], timeout=60)
    client(STACK_DB).flushdb()


def generated_transfers(directory: Path, seed: int = 1, steps: int = 30) -> list[Transfer]:
    """Generated PaySim rows written as a CSV and loaded as the replay loads them, in a new
    namespace."""
    path = directory / "generated.csv"
    write_csv(path, generate(seed, steps))
    return list(load(path, namespace=uuid.uuid4()))
