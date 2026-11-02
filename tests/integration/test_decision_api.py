"""POST /v1/decisions against the running stack (design.md 1.1): the 200's shape, the 400s, a
replay after a restart with another rule set, and keep-alive. Phase 2, criteria 1, 2, 5 and 7."""

from __future__ import annotations

import json
import re
import socket
import time
import uuid
from typing import Any

import httpx
import pytest

from tests.helpers import (
    API_URL,
    CANONICAL_UUID,
    DECISIONS,
    RULE_FIXTURES,
    api_with_rules,
    count,
    decision_request,
    post_decision,
)

FIELDS = {"decisionId", "decision", "ruleSet", "fired", "onTimeout"}


def test_response_shape() -> None:
    """Criterion 1: the five fields, in the formats of design.md 1.1, in at most 16 KiB."""
    response = post_decision(decision_request())
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert len(response.content) <= 16 * 1024
    body = response.json()
    assert set(body) == FIELDS
    assert CANONICAL_UUID.fullmatch(body["decisionId"])
    assert uuid.UUID(body["decisionId"]).version == 4
    assert body["decision"] in DECISIONS
    assert body["onTimeout"] in DECISIONS
    assert re.fullmatch(r"v[1-9][0-9]*", body["ruleSet"])
    assert isinstance(body["fired"], list)
    assert all(isinstance(rule, str) for rule in body["fired"])
    # New accounts and a small amount fire no starter rule: default allow, backup hold.
    assert (body["decision"], body["ruleSet"], body["fired"], body["onTimeout"]) == (
        "allow",
        "v1",
        [],
        "hold",
    )


BAD = {
    "not JSON": b"{oops",
    "an array": b"[]",
    "requestId missing": {k: v for k, v in decision_request().items() if k != "requestId"},
    "requestId with a space": decision_request(requestId="a b"),
    "requestId over 255": decision_request(requestId="k" * 256),
    "fromAccountId not a UUID": decision_request(fromAccountId="nope"),
    "the same account twice": decision_request(
        fromAccountId="7c1e9a52-3a0f-4b8e-9d55-0f3c2b1a6e47",
        toAccountId="7c1e9a52-3a0f-4b8e-9d55-0f3c2b1a6e47",
    ),
    "amount zero": decision_request(amount=0),
    "amount a float": decision_request(amount=12.5),
    "amount a string": decision_request(amount="12500"),
    "amount over 2^63 - 1": decision_request(amount=2**63),
    "currency lowercase": decision_request(currency="cad"),
    "requestedAt without an offset": decision_request(requestedAt="2026-11-20T15:04:05"),
}


@pytest.mark.parametrize("body", BAD.values(), ids=BAD.keys())
def test_bad_requests(body: Any) -> None:
    """Criterion 2: each invalid input gets a 400 problem and leaves no row."""
    rows_before = count("select count(*) from decisions")
    content = body if isinstance(body, bytes) else json.dumps(body).encode()
    response = httpx.post(
        f"{API_URL}/v1/decisions",
        content=content,
        headers={"content-type": "application/json"},
        timeout=5,
    )
    assert response.status_code == 400
    assert response.headers["content-type"] == "application/problem+json"
    problem = response.json()
    assert problem["status"] == 400
    assert {"type", "title", "detail", "errors"} <= set(problem)
    assert count("select count(*) from decisions") == rows_before
    if isinstance(body, dict) and isinstance(body.get("requestId"), str):
        assert (
            count("select count(*) from decisions where request_id = %s", (body["requestId"],)) == 0
        )


# --- Criterion 5: a replay after a restart with another rule set ---------------------------

OTHER_RULES = RULE_FIXTURES / "on_timeout_allow.yaml"


def test_replay_current_on_timeout() -> None:
    body = decision_request()
    first = post_decision(body).json()
    assert first["onTimeout"] == "hold"

    with api_with_rules(OTHER_RULES) as restarted:
        replay = post_decision(body, base_url=restarted)
        fresh = post_decision(decision_request(), base_url=restarted)

    assert replay.status_code == 200
    # The stored decision, with its own ruleSet, and the current rule set's onTimeout (D13).
    assert replay.json() == {**first, "onTimeout": "allow"}
    assert fresh.json()["ruleSet"] == "v2"
    assert fresh.json()["onTimeout"] == "allow"
    # The API that kept running still answers with its own rule set's backup answer.
    assert post_decision(body).json() == first


# --- Criterion 7: keep-alive ---------------------------------------------------------------


def exchange(sock: socket.socket, body: dict[str, Any]) -> int:
    """Sends one POST on an open connection and reads the whole response; returns its status."""
    payload = json.dumps(body).encode()
    sock.sendall(
        b"POST /v1/decisions HTTP/1.1\r\nHost: 127.0.0.1:8000\r\n"
        b"Content-Type: application/json\r\n"
        + f"Content-Length: {len(payload)}\r\n\r\n".encode()
        + payload
    )
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(4096)
        assert chunk, "the server closed the connection"
        data += chunk
    head, _, rest = data.partition(b"\r\n\r\n")
    length = re.search(rb"(?i)content-length:\s*(\d+)", head)
    assert length, "the response has no Content-Length"
    while len(rest) < int(length[1]):
        chunk = sock.recv(4096)
        assert chunk, "the server closed the connection mid-body"
        rest += chunk
    return int(head.split(b" ", 2)[1])


def test_keep_alive() -> None:
    """A connection left idle for 5 s is reused: tally drops its own after 4 s (D13)."""
    with socket.create_connection(("127.0.0.1", 8000), timeout=5) as sock:
        assert exchange(sock, decision_request()) == 200
        time.sleep(5.5)
        assert exchange(sock, decision_request()) == 200
