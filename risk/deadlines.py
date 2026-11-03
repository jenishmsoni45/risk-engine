"""Bounding work by a deadline without waiting for it to unwind.

Shared by the decision API (its 60 ms deadline, design.md section 4) and the ledger stub (its 5 s
bound on its own Postgres, design.md 1.3).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any

log = logging.getLogger(__name__)


class DeadlineExceededError(Exception):
    pass


# Work abandoned at the deadline keeps running until its cancellation completes; held here so it
# isn't garbage-collected first.
_abandoned: set[asyncio.Task[Any]] = set()


def _abandon(task: asyncio.Task[Any]) -> None:
    _abandoned.add(task)
    task.add_done_callback(_forget)


def _forget(task: asyncio.Task[Any]) -> None:
    _abandoned.discard(task)
    if not task.cancelled() and task.exception() is not None:
        log.debug("abandoned work ended with %r", task.exception())


async def within[T](work: Coroutine[Any, Any, T], seconds: float) -> T:
    """The result of work if it finishes within seconds; DeadlineExceededError otherwise.

    The answer never waits for the cancelled work to unwind: psycopg, cancelled mid-query, asks
    the server to cancel and waits up to 5 s for that, which a paused Postgres never answers.
    """
    task = asyncio.ensure_future(work)
    try:
        done, _ = await asyncio.wait({task}, timeout=max(seconds, 0))
    except asyncio.CancelledError:
        # The caller went away. The work finishes on its own.
        _abandon(task)
        raise
    if task in done:
        return task.result()
    task.cancel()
    _abandon(task)
    raise DeadlineExceededError
