"""The ledger stub's HTTP app: tally's transfer path for CI (design.md 1.3, D24).

`POST /transfers` validates, replays a saved key, asks the decision API, and posts on allow (or on
a backup allow), as tally's TransferOrchestrator does. Posting writes the entry and its outbox row
in one transaction; the relay publishes them to ledger.entries.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import psycopg
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import Counter
from psycopg_pool import AsyncConnectionPool

from ledger_stub import store
from ledger_stub.config import StubConfig
from ledger_stub.relay import Relay, producer_config
from ledger_stub.risk_client import ApiConflict, Decided, NoDecision, RiskClient
from ledger_stub.transfers import (
    InvalidTransferError,
    TransferRequest,
    UnreadableTransferError,
    in_record_order,
    parse_transfer,
)
from risk import settings
from risk.checks import check_kafka_topic, check_postgres, health_response, metrics_response
from risk.problems import bad_request, problem

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
# httpx logs every request at INFO: one line per decision call, which the outcome counters cover.
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

INSTANCE = "/transfers"
KEY_REUSED = "Idempotency-Key was already used for a different request"
# Starting values, like tally's Hikari settings: a small pool, and a database too slow for these
# answers 503 rather than hanging the caller.
POOL_MAX_SIZE = 10
POOL_TIMEOUT_S = 2.0
STATEMENT_TIMEOUT_MS = 5000

TRANSFERS = Counter(
    "stub_transfers",
    "POST /transfers by outcome: posted, fallback, replayed, held, blocked, conflict, "
    "risk_unavailable, invalid or database_unavailable",
    ["outcome"],
)
RISK_CALLS = Counter(
    "stub_risk_calls",
    "Decision calls by tally's outcome tag: allow, hold, block, conflict, timeout, unavailable, "
    "invalid",
    ["outcome"],
)
BACKUP_ANSWERS = Counter(
    "stub_backup_answers", "Transfers given the backup answer, by that answer", ["answer"]
)


def create_app(config: StubConfig | None = None) -> FastAPI:
    """The stub; without a config it reads the environment when it starts."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        cfg = config or StubConfig.from_env()
        pool = AsyncConnectionPool(
            cfg.database_url,
            min_size=1,
            max_size=POOL_MAX_SIZE,
            timeout=POOL_TIMEOUT_S,
            kwargs={"autocommit": True, "options": f"-c statement_timeout={STATEMENT_TIMEOUT_MS}"},
            open=False,
            name="stub",
        )
        await pool.open(wait=False)
        producer = Producer(producer_config(cfg.kafka_bootstrap))
        risk = RiskClient(cfg.risk_url, cfg.risk_timeout_s, cfg.on_timeout_default)
        relay = Relay(pool, producer, cfg.relay_poll_s, cfg.relay_batch)
        relay_task = asyncio.create_task(relay.run())
        app.state.database_url = cfg.database_url
        app.state.kafka_admin = AdminClient({"bootstrap.servers": cfg.kafka_bootstrap})
        app.state.pool = pool
        app.state.risk = risk
        app.state.relay = relay
        try:
            yield
        finally:
            relay_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await relay_task
            await risk.aclose()
            await pool.close()
            await asyncio.to_thread(producer.flush, 2.0)

    app = FastAPI(title="ledger stub", lifespan=lifespan)

    @app.post("/transfers")
    async def transfers(request: Request) -> Response:
        try:
            key, transfer = parse_transfer(
                request.headers.get("idempotency-key"), await request.body()
            )
        except UnreadableTransferError:
            TRANSFERS.labels("invalid").inc()
            return problem(400, "Failed to read request", instance=INSTANCE)
        except InvalidTransferError as exc:
            TRANSFERS.labels("invalid").inc()
            return bad_request("Invalid request content.", exc.errors, instance=INSTANCE)
        try:
            return await post_transfer(request.app, key, transfer)
        except psycopg.Error:
            log.warning("database unavailable or too slow; answering 503", exc_info=True)
            TRANSFERS.labels("database_unavailable").inc()
            return problem(
                503,
                "The ledger database is unavailable or too slow; try again. A posting retried "
                "with the same Idempotency-Key moves money at most once.",
                instance=INSTANCE,
            )

    @app.get("/health")
    async def health() -> JSONResponse:
        """200 once Postgres (its keys and outbox) and Kafka (with the topic) answer."""
        postgres_problem, kafka_problem = await asyncio.gather(
            check_postgres(app.state.database_url),
            asyncio.to_thread(check_kafka_topic, app.state.kafka_admin, settings.TOPIC),
        )
        return health_response({"postgres": postgres_problem, "kafka": kafka_problem})

    @app.get("/metrics")
    def metrics() -> Response:
        return metrics_response()

    return app


def replay(saved: store.Saved, request_hash: str) -> Response:
    if saved.request_hash != request_hash:
        TRANSFERS.labels("conflict").inc()
        return problem(409, KEY_REUSED, instance=INSTANCE)
    TRANSFERS.labels("replayed").inc()
    return JSONResponse(in_record_order(saved.body), status_code=saved.status)


async def post(
    app: FastAPI,
    key: str,
    request_hash: str,
    transfer: TransferRequest,
    risk_check: str,
    decision: Decided | None,
) -> Response:
    outcome = await store.post(
        app.state.pool,
        key=key,
        request_hash=request_hash,
        request=transfer,
        risk_check=risk_check,
        decision_id=decision.decision_id if decision else None,
    )
    if isinstance(outcome, store.Saved):
        return replay(outcome, request_hash)
    app.state.relay.wake()
    TRANSFERS.labels("posted" if risk_check == "decided" else "fallback").inc()
    return JSONResponse(outcome.entry, status_code=store.CREATED)


async def post_transfer(app: FastAPI, key: str, transfer: TransferRequest) -> Response:
    request_hash = transfer.request_hash()
    # A saved response is replayed (or is a key conflict) without asking again: the decision
    # API already answered the request that saved it.
    saved = await store.saved(app.state.pool, key)
    if saved is not None:
        return replay(saved, request_hash)

    risk: RiskClient = app.state.risk
    result = await risk.decide(
        key, transfer.from_account_id, transfer.to_account_id, transfer.amount, transfer.currency
    )
    match result:
        case Decided(decision="allow"):
            RISK_CALLS.labels("allow").inc()
            return await post(app, key, request_hash, transfer, "decided", result)
        case Decided(decision="hold" | "block" as refused, decision_id=decision_id):
            RISK_CALLS.labels(refused).inc()
            TRANSFERS.labels("held" if refused == "hold" else "blocked").inc()
            # The decision id lets support find the decision; the fired rules stay with the
            # decision API, so nobody can tune a transfer against them (D3).
            if refused == "hold":
                return problem(
                    403,
                    "The risk check held this transfer for review.",
                    type_="/problems/transfer-held",
                    title="Transfer held for review",
                    decisionId=str(decision_id),
                    instance=INSTANCE,
                )
            return problem(
                403,
                "The risk check blocked this transfer.",
                type_="/problems/transfer-blocked",
                title="Transfer blocked",
                decisionId=str(decision_id),
                instance=INSTANCE,
            )
        case ApiConflict():
            RISK_CALLS.labels("conflict").inc()
            TRANSFERS.labels("conflict").inc()
            return problem(409, KEY_REUSED, instance=INSTANCE)
        case NoDecision(reason=reason):
            RISK_CALLS.labels(reason).inc()
            answer = risk.backup_answer
            BACKUP_ANSWERS.labels(answer).inc()
            log.warning(
                "risk check gave no decision, outcome=%s; using the backup answer %s "
                "idempotencyKey=%s",
                reason,
                answer,
                key,
            )
            if answer == "allow":
                return await post(app, key, request_hash, transfer, "fallback", None)
            # A backup refusal is a 503, not a 403: nothing was decided, and a retry with the
            # same key asks again (D6).
            TRANSFERS.labels("risk_unavailable").inc()
            return problem(
                503,
                "The risk check did not answer in time; try again. A transfer retried with the "
                "same Idempotency-Key moves money at most once.",
                type_="/problems/risk-unavailable",
                title="Risk check unavailable",
                instance=INSTANCE,
            )
    raise AssertionError(f"unhandled decision result {result!r}")


app = create_app()
