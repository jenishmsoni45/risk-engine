"""Shared fixtures, and the one Hypothesis profile every run uses (design.md section 11).

The profile is derandomized, so a run generates the same examples everywhere and any failure
replays; Hypothesis prints the failing example. No example database is written.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
import redis
from hypothesis import settings

from tests.feature_state import SCRATCH_DB, client
from tests.helpers import compose_ps

settings.register_profile("repo", derandomize=True, database=None, print_blob=True, deadline=None)
settings.load_profile("repo")


@pytest.fixture(scope="module")
def scratch() -> Iterator[redis.Redis]:
    """Database 15 of the stack's Redis, which no service uses, emptied before and after. A
    property test empties it again for each example."""
    db = client(SCRATCH_DB)
    db.flushdb()
    yield db
    db.flushdb()
    db.close()


@pytest.fixture(scope="session")
def stack() -> None:
    """Fails the test at once when the Compose stack isn't up, rather than timing out later."""
    running = [row["Service"] for row in compose_ps() if row.get("State") == "running"]
    if not running:
        pytest.fail("the Compose stack isn't running: start it with `make up` (or run `make test`)")
