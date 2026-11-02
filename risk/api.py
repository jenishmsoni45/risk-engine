"""The decision API (architecture.md section 1, design.md 1.1 and section 4).

Run with `python -m risk.api`, which serves on port 8000 with the keep-alive of D40e.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Coroutine
from contextlib import asynccontextmanager
from typing import Any

import psycopg
import redis.asyncio as aioredis
import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import Counter
from psycopg_pool import AsyncConnectionPool

from risk import settings
from risk.checks import (
    CHECK_TIMEOUT_S,
    check_postgres,
    check_redis,
    health_response,
    metrics_response,
)
from risk.decisions import (
    Conflict,
    CorruptWatermarkError,
    DecisionEngine,
    InvalidRequestError,
    parse_request,
)
from risk.problems import bad_request, problem
from risk.rules import ruleset

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger(__name__)

PORT = 8000
# Starting values, not measurements: enough connections for the requests one process has in
# flight, few enough to leave Postgres room for the stub and the consumer. Phase 5 measures.
POOL_MIN_SIZE = 2
POOL_MAX_SIZE = 20

REQUESTS = Counter(
    "risk_decision_requests",
    "Decision requests by outcome: decided, replayed, conflict, invalid or unavailable (503)",
    ["outcome"],
)
DECISIONS = Counter("risk_decisions", "New decisions stored, by decision", ["decision"])

# Failures of Redis or Postgres, which answer 503 (D22). TimeoutError is an OSError.
DEPENDENCY_ERRORS: tuple[type[BaseException], ...] = (
    psycopg.Error,
    aioredis.RedisError,
    OSError,
    CorruptWatermarkError,
)


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
        log.debug("abandoned decision work ended with %r", task.exception())


async def within[T](work: Coroutine[Any, Any, T], seconds: float) -> T:
    """The result of work if it finishes within seconds; DeadlineExceededError otherwise.

    The answer never waits for the cancelled work to unwind: psycopg, cancelled mid-query, asks
    the server to cancel and waits up to 5 s for that, which a paused Postgres never answers.
    """
    task = asyncio.ensure_future(work)
    try:
        done, _ = await asyncio.wait({task}, timeout=max(seconds, 0))
    except asyncio.CancelledError:
        # The client went away. The decision finishes on its own and a retry replays it.
        _abandon(task)
        raise
    if task in done:
        return task.result()
    task.cancel()
    _abandon(task)
    raise DeadlineExceededError


def unavailable(reason: str) -> JSONResponse:
    REQUESTS.labels("unavailable").inc()
    log.warning("decision unavailable: %s", reason)
    return problem(
        503,
        "The decision could not be completed in time; nothing was stored. Ask again.",
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # An invalid rule file stops the service here, before it serves anything (D27).
    rules = ruleset.load(settings.rules_path())
    log.info(
        "rule set %s loaded from %s, sha256 %s", rules.name, settings.rules_path(), rules.sha256
    )
    deadline_ms = settings.decision_deadline_ms()
    deadline_s = deadline_ms / 1000
    database_url = settings.database_url()
    redis_url = settings.redis_url()

    # Every Redis and Postgres call carries its own timeout, no longer than the deadline
    # (design.md section 4, step 2): the socket timeout, the wait for a pooled connection, and
    # statement_timeout on the server.
    pool = AsyncConnectionPool(
        database_url,
        min_size=POOL_MIN_SIZE,
        max_size=POOL_MAX_SIZE,
        timeout=deadline_s,
        kwargs={"autocommit": True, "options": f"-c statement_timeout={deadline_ms}"},
        open=False,
        name="decisions",
    )
    # Not waiting: the pool fills in the background, and /health reports Postgres until it can.
    await pool.open(wait=False)
    redis = aioredis.Redis.from_url(
        redis_url, socket_timeout=deadline_s, socket_connect_timeout=deadline_s
    )
    health_redis = aioredis.Redis.from_url(
        redis_url, socket_timeout=CHECK_TIMEOUT_S, socket_connect_timeout=CHECK_TIMEOUT_S
    )
    app.state.database_url = database_url
    app.state.health_redis = health_redis
    app.state.rules = rules
    app.state.deadline_s = deadline_s
    app.state.engine = DecisionEngine(rules, pool, redis)
    try:
        yield
    finally:
        await pool.close()
        await redis.aclose()
        await health_redis.aclose()


app = FastAPI(title="risk engine decision API", lifespan=lifespan)


@app.post("/v1/decisions")
async def decide(request: Request) -> Response:
    started = time.monotonic()
    try:
        parsed = parse_request(await request.body())
    except InvalidRequestError as exc:
        REQUESTS.labels("invalid").inc()
        return bad_request("The decision request is invalid.", exc.errors)

    state = request.app.state
    remaining = state.deadline_s - (time.monotonic() - started)
    try:
        outcome = await within(state.engine.decide(parsed), remaining)
    except DeadlineExceededError:
        return unavailable("the deadline passed")
    except DEPENDENCY_ERRORS as exc:
        return unavailable(repr(exc))

    if isinstance(outcome, Conflict):
        REQUESTS.labels("conflict").inc()
        return problem(409, "This requestId was already decided for a different transfer.")
    if outcome.replayed:
        REQUESTS.labels("replayed").inc()
    else:
        REQUESTS.labels("decided").inc()
        DECISIONS.labels(outcome.decision).inc()
    # onTimeout is always the current rule set's, a replay's included (D13).
    return JSONResponse(
        {
            "decisionId": str(outcome.decision_id),
            "decision": outcome.decision,
            "ruleSet": outcome.rule_set,
            "fired": outcome.fired,
            "onTimeout": state.rules.on_timeout,
        }
    )


@app.get("/health")
async def health() -> JSONResponse:
    """200 once Redis and Postgres answer (design.md 1.1)."""
    redis_problem, postgres_problem = await asyncio.gather(
        check_redis(app.state.health_redis), check_postgres(app.state.database_url)
    )
    return health_response({"redis": redis_problem, "postgres": postgres_problem})


@app.get("/metrics")
def metrics() -> Response:
    return metrics_response()


def main() -> None:
    uvicorn.run(
        app,
        # Every interface inside the container; Compose publishes the port on 127.0.0.1 only.
        host="0.0.0.0",  # noqa: S104
        port=PORT,
        # Idle connections stay open this long (D40e); tally needs at least 5 s (D13).
        timeout_keep_alive=settings.keep_alive_seconds(),
    )


if __name__ == "__main__":
    main()
