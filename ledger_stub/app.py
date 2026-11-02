"""The ledger stub's HTTP app.

Phase 1 serves only /health and /metrics; Phase 2 adds POST /transfers.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from confluent_kafka.admin import AdminClient
from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse

from risk import settings
from risk.checks import check_kafka_topic, check_postgres, health_response, metrics_response

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.database_url = settings.database_url()
    app.state.kafka_admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap()})
    yield


app = FastAPI(title="ledger stub", lifespan=lifespan)


@app.get("/health")
async def health() -> JSONResponse:
    """200 once Postgres (its keys and outbox) and Kafka (with the topic it publishes to) answer."""
    postgres_problem, kafka_problem = await asyncio.gather(
        check_postgres(app.state.database_url),
        asyncio.to_thread(check_kafka_topic, app.state.kafka_admin, settings.TOPIC),
    )
    return health_response({"postgres": postgres_problem, "kafka": kafka_problem})


@app.get("/metrics")
def metrics() -> Response:
    return metrics_response()
