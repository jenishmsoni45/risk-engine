"""With Redis or Postgres paused, the decision API answers 503 within its 60 ms deadline and stores
nothing (D22, D40d), so the caller gets its backup answer well before its own 100 ms timeout.
Phase 2, criterion 6. Timing margin: D40m's 100 ms.
"""

from __future__ import annotations

import time

import httpx
import pytest

from tests.helpers import (
    API_URL,
    TIMING_MARGIN_S,
    decision_request,
    decision_rows,
    paused,
    post_decision,
)

DEADLINE_S = 0.060


@pytest.mark.parametrize("service", ["redis", "postgres"])
def test_paused_dependency_gives_503_within_the_deadline(service: str) -> None:
    body = decision_request()
    with httpx.Client(base_url=API_URL, timeout=5) as client:
        # An open connection, so the time measured is the API's and not a TCP handshake.
        assert client.get("/metrics").status_code == 200
        with paused(service):
            started = time.monotonic()
            response = client.post("/v1/decisions", json=body)
            elapsed = time.monotonic() - started

    assert response.status_code == 503
    assert response.headers["content-type"] == "application/problem+json"
    assert elapsed <= DEADLINE_S + TIMING_MARGIN_S, f"answered after {elapsed * 1000:.0f} ms"
    # Checked once the service is back and healthy: the abandoned work was cancelled before its
    # insert, so no row appeared later either.
    assert decision_rows(body["requestId"]) == []

    # Once the service is back, the same request is decided afresh.
    retried = post_decision(body)
    assert retried.status_code == 200
    assert len(decision_rows(body["requestId"])) == 1
