"""The decision API (architecture.md section 1, design.md 1.1 and section 4).

Run with `python -m risk.api`, which serves on port 8000 with the keep-alive of D40e.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import psycopg
import redis.asyncio as aioredis
import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import Counter
from psycopg_pool import AsyncConnectionPool

from risk import settings, shadow
from risk.checks import (
    CHECK_TIMEOUT_S,
    check_postgres,
    check_redis,
    health_response,
    metrics_response,
)
from risk.deadlines import DeadlineExceededError, within
from risk.decisions import (
    Conflict,
    DecisionEngine,
    DecisionRequest,
    InvalidRequestError,
    parse_request,
)
from risk.features.redis_store import CorruptFeatureDataError, FeatureStore
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
ATTEMPT_WRITE_FAILURES = Counter(
    "risk_attempt_write_failures",
    "Attempts not recorded after their decision committed; the decision was still returned",
)

# Failures of Redis or Postgres, which answer 503 (D22). TimeoutError is an OSError.
DEPENDENCY_ERRORS: tuple[type[BaseException], ...] = (
    psycopg.Error,
    aioredis.RedisError,
    OSError,
    CorruptFeatureDataError,
)


def unavailable(reason: str) -> JSONResponse:
    REQUESTS.labels("unavailable").inc()
    log.warning("decision unavailable: %s", reason)
    return problem(
        503,
        "The decision could not be completed in time; nothing was stored. Ask again.",
    )


async def record_attempt(engine: DecisionEngine, request: DecisionRequest) -> None:
    """design.md 3.3: past the commit the deadline no longer applies, and a failed write still
    returns the decision; it is counted, and the rebuild (design.md 3.5) restores the attempt.
    The Redis client's own timeout bounds the write."""
    try:
        await engine.record_attempt(request)
    except DEPENDENCY_ERRORS as exc:
        ATTEMPT_WRITE_FAILURES.inc()
        log.warning("attempt for %r not recorded: %r", request.request_id, exc)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # An invalid rule file stops the service here, before it serves anything (D27).
    try:
        rules = ruleset.load(settings.rules_path())
    except ruleset.RuleSetError as exc:
        log.error("invalid rule file, not starting: %s", exc)
        raise
    log.info(
        "rule set %s with %d rules loaded from %s, sha256 %s",
        rules.name,
        len(rules.rules),
        settings.rules_path(),
        rules.sha256,
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
    # 2b (D39, D58): a model that fails its checks is logged, and the API starts without it.
    scorer = shadow.start(settings.shadow_model_dir(), settings.shadow_timeout_ms() / 1000)
    app.state.engine = DecisionEngine(
        rules, pool, FeatureStore(redis, settings.lateness_margin_ms()), scorer
    )
    try:
        yield
    finally:
        if scorer is not None:
            scorer.close()
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
        await record_attempt(state.engine, parsed)
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
        # No log line per request: the counters count them, and the bench measures the service
        # rather than its logging (D48.5).
        access_log=False,
    )


if __name__ == "__main__":
    main()
