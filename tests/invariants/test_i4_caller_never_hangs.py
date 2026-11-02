"""I4: past its timeout, the caller applies on_timeout and never hangs (design.md section 6).

Whatever the decision API does, the stub's answer comes within its 100 ms timeout plus D40m's
100 ms margin, and it is the backup answer: 503 risk-unavailable under the default hold, or a
posting with riskCheck fallback once a reply has named allow. Phase 2, criterion 11.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

import httpx
import pytest

from tests.fake_risk import FakeRisk, Responder, decided, headers_only, late, raw, silent
from tests.helpers import STUB_URL, TIMING_MARGIN_S, count, free_port, paused, unique
from tests.stub_process import running_stub

LIMIT_S = 0.100 + TIMING_MARGIN_S


def reply_body(**overrides: Any) -> bytes:
    body = {
        "decisionId": str(uuid.uuid4()),
        "decision": "allow",
        "ruleSet": "v1",
        "fired": [],
        "onTimeout": "hold",
        **overrides,
    }
    return json.dumps(body).encode()


MISBEHAVIOURS: dict[str, Responder] = {
    "never answers": silent,
    "answers late": late(0.3, decided("allow")),
    "sends headers and no body": headers_only,
    "answers 500": raw(500, b'{"status":500}'),
    # A valid decision in every way but its size.
    "sends a body over 16 KiB": raw(200, reply_body(fired=["x" * 17_000])),
    "sends invalid JSON": raw(200, b'{"decisionId": '),
    "sends an invalid decision": raw(200, reply_body(decision="ALLOW")),
}


def transfer() -> dict[str, Any]:
    return {
        "fromAccountId": str(uuid.uuid4()),
        "toAccountId": str(uuid.uuid4()),
        "amount": 500,
        "currency": "CAD",
    }


def timed_post(client: httpx.Client, key: str) -> tuple[httpx.Response, float]:
    started = time.monotonic()
    response = client.post("/transfers", headers={"Idempotency-Key": key}, json=transfer())
    return response, time.monotonic() - started


def assert_risk_unavailable(response: httpx.Response, key: str) -> None:
    assert response.status_code == 503, response.text
    assert response.json()["type"] == "/problems/risk-unavailable"
    # Nothing was posted, and no key saved: a retry asks again (D6).
    assert count("select count(*) from stub.transfers where idempotency_key = %s", (key,)) == 0


@pytest.mark.parametrize("responder", MISBEHAVIOURS.values(), ids=MISBEHAVIOURS.keys())
def test_default_hold_answers_risk_unavailable_in_time(responder: Responder) -> None:
    with FakeRisk() as fake, running_stub(fake.url) as url, httpx.Client(base_url=url) as client:
        fake.respond_with(responder)
        key = unique("i4-hold")
        response, elapsed = timed_post(client, key)
    assert elapsed <= LIMIT_S, f"answered after {elapsed * 1000:.0f} ms"
    assert_risk_unavailable(response, key)
    assert fake.calls == 1


@pytest.mark.parametrize("responder", MISBEHAVIOURS.values(), ids=MISBEHAVIOURS.keys())
def test_after_on_timeout_allow_posts_with_fallback_in_time(responder: Responder) -> None:
    with FakeRisk() as fake, running_stub(fake.url) as url, httpx.Client(base_url=url) as client:
        fake.respond_with(decided("allow", on_timeout="allow"))
        first, _ = timed_post(client, unique("i4-first"))
        assert first.json()["riskCheck"] == "decided"
        fake.respond_with(responder)
        response, elapsed = timed_post(client, unique("i4-fallback"))
    assert elapsed <= LIMIT_S, f"answered after {elapsed * 1000:.0f} ms"
    assert response.status_code == 201, response.text
    entry = response.json()
    assert entry["riskCheck"] == "fallback"
    assert entry["decisionId"] is None


def test_connection_refused() -> None:
    nothing_listening = f"http://127.0.0.1:{free_port()}"
    with running_stub(nothing_listening) as url, httpx.Client(base_url=url) as client:
        key = unique("i4-refused")
        response, elapsed = timed_post(client, key)
    assert elapsed <= LIMIT_S
    assert_risk_unavailable(response, key)


def test_the_decision_api_paused() -> None:
    """The stub container against the real API container, paused."""
    with httpx.Client(base_url=STUB_URL, timeout=5) as client:
        # An open connection, so the time measured is the stub's and not a TCP handshake.
        assert client.get("/metrics").status_code == 200
        key = unique("i4-paused")
        with paused("api"):
            response, elapsed = timed_post(client, key)
    assert elapsed <= LIMIT_S, f"answered after {elapsed * 1000:.0f} ms"
    assert_risk_unavailable(response, key)
