"""The stack made ready for a replay (D52b), and generated PaySim transfers to replay (D37).

The replay waits for the watermark to reach each transfer's time, so the watermark must start
behind the first one. On the normal stack it follows the wall clock, ahead of D51.2's January, so
the tests that replay wait until the consumer has applied every event already on the topic, then
empty the stack's Redis, as tests/adversarial/test_rebuild.py does. Each replay maps PaySim names
in a new namespace, so its keys and accounts never meet an earlier run's rows in Postgres.
"""

from __future__ import annotations

import uuid
from pathlib import Path

from ml.generate import generate, write_csv
from ml.paysim import Transfer, load
from tests.feature_state import STACK_DB, client
from tests.helpers import topic_ends, wait_until_consumed


def fresh_watermark() -> None:
    """The stack's Redis emptied once the consumer has caught up with ledger.entries, so no
    event published earlier can raise the watermark afterwards."""
    ends = topic_ends()
    wait_until_consumed([(p, end - 1) for p, end in ends.items() if end > 0], timeout=60)
    client(STACK_DB).flushdb()


def generated_transfers(directory: Path, seed: int = 1, steps: int = 30) -> list[Transfer]:
    """Generated PaySim rows written as a CSV and loaded as the replay loads them, in a new
    namespace."""
    path = directory / "generated.csv"
    write_csv(path, generate(seed, steps))
    return list(load(path, namespace=uuid.uuid4()))
