"""I8 (design.md section 6): shadow scoring never changes a live decision. Phase 8, criterion 1.

1. In this process (D58i), since LightGBM never loads on the Mac (D51.4): the decision API's own
   handler decides the same Hypothesis-generated requests over the same history once per scorer:
   shadow off, a working stand-in model, one that raises, one that returns nonsense, and one
   slower than the handler's deadline. Each scorer gets its own copy of the accounts, so the
   features are identical and the decisions independent. Every answer is a 200, and decision,
   ruleSet and fired are the same with every scorer. A slow model can't turn a decision into a
   503, because the handler waits at most SHADOW_TIMEOUT_MS for it (D57.1). The rows hold the
   working model's score and no score from the others.
2. In the containers: the stack's API, scoring the real model, and a second API with shadow off
   decide the same requests over the same history, published through Kafka, with every starter
   rule firing. The answers are the same; the first API's rows hold the model's name and a
   score, the second's hold neither.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import redis
from hypothesis import given, settings
from hypothesis import strategies as st

from risk.features.compute import FEATURE_NAMES
from risk.shadow import Shadow
from tests.feature_state import SCRATCH_DB, deliver, sample
from tests.helpers import (
    API_URL,
    ROOT,
    database,
    iso_ms,
    post_decision,
    publish,
    second_api,
    unique,
    wait_until_consumed,
)
from tests.ledger_events import ACCOUNTS, HOUR_US, MINUTE_US, Event, event_logs
from tests.oracle import OracleLine
from tests.rule_scenario import STARTER_RULE_IDS, Scenario
from tests.shadow_fakes import (
    SLOW_S,
    Rows,
    decision_api,
    nonsense,
    raising,
    slow,
    stand_in,
    working,
)

START_US = 1_793_000_000 * 1_000_000  # 2026-10-27, an arbitrary fixed start
START_MS = START_US // 1000
# Room for Docker's port forwarding between this process and the stack, yet under the slow
# stand-in's sleep, so a decision that waited for it would answer 503.
DEADLINE_S = 0.5
assert SLOW_S > DEADLINE_S
FAILURES = "risk_shadow_failures_total"

# Each scorer's stand-in answer; None is shadow off.
SCORERS: dict[str, Callable[[Rows], object] | None] = {
    "off": None,
    "working": working,
    "raises": raising,
    "nonsense": nonsense,
    "slow": slow,
}
# The reason each failing scorer's decisions are counted under.
COUNTED = {"raises": "error", "nonsense": "invalid", "slow": "timeout"}


def scorer(name: str) -> Shadow | None:
    answer = SCORERS[name]
    return None if answer is None else stand_in(answer)


@dataclass(frozen=True)
class Request:
    sender: uuid.UUID
    receiver: uuid.UUID
    amount: int
    t_ms: int


@st.composite
def request_runs(draw: st.DrawFn) -> list[Request]:
    """Up to 12 requests among the four accounts, within two minutes after the history's first
    hour, so attempts and the history's later events can count; amounts straddle the starter
    rules' cent thresholds."""
    count = draw(st.integers(1, 12))
    run = []
    for _ in range(count):
        sender, receiver = draw(
            st.lists(st.sampled_from(ACCOUNTS), min_size=2, max_size=2, unique=True)
        )
        amount = draw(st.one_of(st.integers(1, 3_000_000), st.integers(1, 2**62)))
        t_ms = draw(st.integers(START_MS + 60 * 60_000, START_MS + 62 * 60_000))
        run.append(Request(sender, receiver, amount, t_ms))
    return sorted(run, key=lambda r: r.t_ms)


def renamed(event: Event, namespace: uuid.UUID) -> Event:
    """The event with its id and accounts moved into namespace."""
    return dataclasses.replace(
        event,
        id=uuid.uuid5(namespace, str(event.id)),
        lines=tuple(
            OracleLine(uuid.uuid5(namespace, str(line.account)), line.amount)
            for line in event.lines
        ),
    )


def body(request: Request, namespace: uuid.UUID) -> dict[str, Any]:
    return {
        "requestId": unique("i8"),
        "fromAccountId": str(uuid.uuid5(namespace, str(request.sender))),
        "toAccountId": str(uuid.uuid5(namespace, str(request.receiver))),
        "amount": request.amount,
        "currency": "CAD",
        "requestedAt": iso_ms(request.t_ms),
    }


async def decide_all(shadow: Shadow | None, bodies: Sequence[dict[str, Any]]) -> list[Any]:
    async with decision_api(shadow, SCRATCH_DB, DEADLINE_S) as client:
        answers = []
        for one in bodies:
            response = await client.post("/v1/decisions", json=one)
            assert response.status_code == 200, response.text
            answers.append(response.json())
        return answers


def shadow_columns(request_ids: Sequence[str]) -> list[tuple[Any, ...]]:
    with database() as conn:
        rows = conn.execute(
            "select request_id, features, shadow_score, shadow_model from decisions "
            "where request_id = any(%s)",
            (list(request_ids),),
        ).fetchall()
    by_id = {row[0]: row[1:] for row in rows}
    return [by_id[request_id] for request_id in request_ids]


def without_id(answer: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in answer.items() if key != "decisionId"}


@settings(max_examples=25)
@given(
    history=event_logs(st.integers(START_US, START_US + HOUR_US + 2 * MINUTE_US), max_size=25),
    run=request_runs(),
)
def test_no_scorer_changes_a_decision(
    scratch: redis.Redis, history: list[Event], run: list[Request]
) -> None:
    scratch.flushdb()
    namespaces = {name: uuid.uuid4() for name in SCORERS}
    log = [renamed(event, ns) for ns in namespaces.values() for event in history]
    deliver(scratch, log, range(len(log)))

    counted_before = {name: sample(FAILURES, reason=reason) for name, reason in COUNTED.items()}
    bodies = {name: [body(r, ns) for r in run] for name, ns in namespaces.items()}
    answers = {name: asyncio.run(decide_all(scorer(name), bodies[name])) for name in SCORERS}

    expected = [without_id(answer) for answer in answers["off"]]
    for name, given_answers in answers.items():
        assert [without_id(answer) for answer in given_answers] == expected, name
    for name, reason in COUNTED.items():
        assert sample(FAILURES, reason=reason) == counted_before[name] + len(run), name

    for name, answer in SCORERS.items():
        for features, score, model in shadow_columns([one["requestId"] for one in bodies[name]]):
            if answer is None:
                assert (score, model) == (None, None)
                continue
            assert model == f"stand-in-{answer.__name__}"
            if answer is working:
                row = np.array([[features[f] for f in FEATURE_NAMES]], dtype=np.float64)
                assert score == float(np.asarray(working(row))[0])
            else:
                assert score is None


def stored(request_ids: Sequence[str]) -> list[tuple[Any, ...]]:
    return [(score, model) for _, score, model in shadow_columns(request_ids)]


def test_the_real_model_changes_no_decision() -> None:
    meta = json.loads((ROOT / "ml" / "model" / "model.json").read_text())
    on, off = Scenario(), Scenario()
    events = [(event.key(), event.value()) for s in (on, off) for event in s.events()]
    wait_until_consumed(publish(events))
    on_bodies, off_bodies = on.requests(), off.requests()

    with second_api(env={"SHADOW_MODEL_DIR": ""}) as shadow_off:
        answers = {}
        for name, url, bodies in (
            ("on", API_URL, on_bodies),
            ("off", shadow_off.url, off_bodies),
        ):
            responses = [post_decision(one, base_url=url) for one in bodies]
            assert [r.status_code for r in responses] == [200] * len(bodies), name
            answers[name] = [without_id(r.json()) for r in responses]

    assert answers["on"] == answers["off"]
    assert {a["decision"] for a in answers["on"]} == {"allow", "hold", "block"}
    assert {rule for a in answers["on"] for rule in a["fired"]} == STARTER_RULE_IDS

    on_rows = stored([one["requestId"] for one in on_bodies])
    assert {model for _, model in on_rows} == {meta["name"]}
    assert all(isinstance(score, float) and 0 <= score <= 1 for score, _ in on_rows)
    assert stored([one["requestId"] for one in off_bodies]) == [(None, None)] * len(off_bodies)
