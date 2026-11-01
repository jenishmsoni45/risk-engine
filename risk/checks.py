"""Dependency checks behind GET /health, and the Prometheus text for GET /metrics (D40t).

Each check returns None when the dependency answered, or a short reason when it didn't. The
reason is logged; the health body names only the dependency, never connection details.
"""

from __future__ import annotations

import asyncio
import logging

import psycopg
import redis.asyncio as aioredis
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from fastapi import Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest

log = logging.getLogger(__name__)

CHECK_TIMEOUT_S = 2.0


async def check_redis(client: aioredis.Redis) -> str | None:
    try:
        async with asyncio.timeout(CHECK_TIMEOUT_S):
            await client.ping()
    except (TimeoutError, OSError, aioredis.RedisError) as exc:
        return f"redis: {exc!r}"
    return None


async def check_postgres(conninfo: str) -> str | None:
    try:
        async with asyncio.timeout(CHECK_TIMEOUT_S):
            conn = await psycopg.AsyncConnection.connect(conninfo, connect_timeout=2)
            async with conn:
                await conn.execute("select 1")
    except (TimeoutError, OSError, psycopg.Error) as exc:
        return f"postgres: {exc!r}"
    return None


def check_kafka_topic(admin: AdminClient, topic: str) -> str | None:
    """Blocking: the broker answers metadata and the topic exists. Run it in a thread."""
    try:
        metadata = admin.list_topics(topic=topic, timeout=CHECK_TIMEOUT_S)
    except KafkaException as exc:
        return f"kafka: {exc!r}"
    found = metadata.topics.get(topic)
    if found is None or found.error is not None:
        return f"kafka: topic {topic} unavailable ({found.error if found else 'missing'})"
    return None


def health_response(results: dict[str, str | None]) -> JSONResponse:
    """200 when every check passed; otherwise 503 naming the dependencies that failed."""
    failed = sorted(name for name, problem in results.items() if problem is not None)
    for name in failed:
        log.warning("health check failed: %s", results[name])
    if failed:
        return JSONResponse({"status": "unavailable", "failed": failed}, status_code=503)
    return JSONResponse({"status": "ok"})


def metrics_response() -> Response:
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
