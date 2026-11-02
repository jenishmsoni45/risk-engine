"""The decision API (architecture.md section 1). Phase 1 serves only /health and /metrics."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse

from risk import settings
from risk.checks import (
    CHECK_TIMEOUT_S,
    check_postgres,
    check_redis,
    health_response,
    metrics_response,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.database_url = settings.database_url()
    app.state.redis = aioredis.Redis.from_url(
        settings.redis_url(),
        socket_timeout=CHECK_TIMEOUT_S,
        socket_connect_timeout=CHECK_TIMEOUT_S,
    )
    try:
        yield
    finally:
        await app.state.redis.aclose()


app = FastAPI(title="risk engine decision API", lifespan=lifespan)


@app.get("/health")
async def health() -> JSONResponse:
    """200 once Redis and Postgres answer (design.md 1.1)."""
    redis_problem, postgres_problem = await asyncio.gather(
        check_redis(app.state.redis), check_postgres(app.state.database_url)
    )
    return health_response({"redis": redis_problem, "postgres": postgres_problem})


@app.get("/metrics")
def metrics() -> Response:
    return metrics_response()
