"""The Compose stack: health, ports, pinned images, the topic, /health and /metrics (Phase 1)."""

from __future__ import annotations

import json
import re
from typing import Any

import httpx
import pytest
from confluent_kafka.admin import AdminClient
from prometheus_client.parser import text_string_to_metric_families

from risk.settings import TOPIC
from tests.helpers import (
    API_URL,
    CONSUMER_METRICS_URL,
    KAFKA_HOST_BOOTSTRAP,
    ROOT,
    STUB_URL,
    compose,
    compose_ps,
)

LONG_RUNNING = {"postgres", "redis", "kafka", "api", "consumer", "stub"}
ONE_SHOT = {"migrate", "topics"}

# D31's table plus the consumer's metrics port (D40z): service -> (host port, container port).
EXPECTED_PORTS = {
    "api": (8000, 8000),
    "stub": (8001, 8001),
    "consumer": (8002, 8002),
    "postgres": (5433, 5432),
    "redis": (6379, 6379),
    "kafka": (19092, 19092),
}

APP_IMAGE = "risk-engine:local"
# An exact release: a version with at least two numeric parts, then optional variants
# (postgres:18.6, apache/kafka:4.3.1, python:3.12.16-slim-trixie).
EXACT_TAG = re.compile(r"[a-z0-9]+(?:[./-][a-z0-9]+)*:\d+(?:\.\d+)+(?:-[a-z0-9.]+)*")


def compose_config() -> dict[str, Any]:
    config: dict[str, Any] = json.loads(compose("config", "--format", "json").stdout)
    return config


def test_services_healthy() -> None:
    rows = {row["Service"]: row for row in compose_ps()}
    assert set(rows) == LONG_RUNNING | ONE_SHOT
    for service in sorted(LONG_RUNNING):
        assert rows[service]["State"] == "running", service
        assert rows[service]["Health"] == "healthy", service
    for service in sorted(ONE_SHOT):
        assert rows[service]["State"] == "exited", service
        assert rows[service]["ExitCode"] == 0, service


def test_ports() -> None:
    published: dict[str, list[tuple[str, int, int]]] = {}
    for name, service in compose_config()["services"].items():
        for port in service.get("ports", []):
            published.setdefault(name, []).append(
                # No host_ip means every interface, which the comparison below refuses.
                (port.get("host_ip", "0.0.0.0"), int(port["published"]), int(port["target"]))  # noqa: S104
            )
    expected = {
        name: [("127.0.0.1", host, container)] for name, (host, container) in EXPECTED_PORTS.items()
    }
    assert published == expected


def test_images_pinned() -> None:
    services = compose_config()["services"]
    for name, service in services.items():
        image = service["image"]
        assert not image.endswith(":latest"), name
        if "build" in service:
            assert image == APP_IMAGE, name
        else:
            assert EXACT_TAG.fullmatch(image), f"{name}: {image} has no exact tag"
    base_images = re.findall(r"^FROM\s+(\S+)", (ROOT / "Dockerfile").read_text(), re.MULTILINE)
    assert base_images
    for image in base_images:
        assert EXACT_TAG.fullmatch(image), f"Dockerfile: {image} has no exact tag"


def test_topic() -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA_HOST_BOOTSTRAP})
    metadata = admin.list_topics(topic=TOPIC, timeout=10)
    topic = metadata.topics[TOPIC]
    assert topic.error is None
    assert len(topic.partitions) == 3


@pytest.mark.parametrize("base_url", [API_URL, STUB_URL])
def test_health(base_url: str) -> None:
    response = httpx.get(f"{base_url}/health", timeout=5)
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.parametrize("base_url", [API_URL, STUB_URL, CONSUMER_METRICS_URL])
def test_health_and_metrics(base_url: str) -> None:
    response = httpx.get(f"{base_url}/metrics", timeout=5)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    families = list(text_string_to_metric_families(response.text))
    assert families, "no metric families in the response"
