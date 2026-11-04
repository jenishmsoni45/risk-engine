"""The replay's own checks (ml/replay.py, D36, D52h), without a stack: what it sends, when it
refuses to start, and how long it waits for the watermark."""

from __future__ import annotations

import uuid

import pytest

from ml.paysim import STEP_1_MS, Transfer
from ml.replay import WAIT_TIMEOUT_S, ReplayError, check_order, replay, request, wait_for

A, B = uuid.UUID(int=1), uuid.UUID(int=2)


def transfer(time_ms: int, line: int = 2) -> Transfer:
    return Transfer(
        line=line,
        step=1,
        type="CASH_OUT",
        request_id=str(uuid.uuid4()),
        sender=A,
        receiver=B,
        amount=1999,
        time_ms=time_ms,
        is_fraud=False,
    )


class FixedWatermark:
    def __init__(self, value: int) -> None:
        self.value = value
        self.reads = 0

    def read(self) -> int:
        self.reads += 1
        return self.value


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def test_each_transfer_is_posted_with_its_key_and_replay_time() -> None:
    t = transfer(STEP_1_MS + 1_234)
    headers, body = request(t)
    assert headers == {"Idempotency-Key": t.request_id, "X-Replay-Time": "2026-01-01T00:00:01.234Z"}
    assert body == {
        "fromAccountId": str(A),
        "toAccountId": str(B),
        "amount": 1999,
        "currency": "CAD",
    }


def test_times_that_dont_strictly_increase_are_refused() -> None:
    check_order([transfer(STEP_1_MS), transfer(STEP_1_MS + 1)])
    with pytest.raises(ReplayError, match="line 9: times must strictly increase"):
        check_order([transfer(STEP_1_MS), transfer(STEP_1_MS, line=9)])


def test_a_stack_whose_watermark_is_at_the_first_transfer_is_refused() -> None:
    watermark = FixedWatermark(STEP_1_MS)
    with pytest.raises(ReplayError, match="needs a fresh stack"):
        # Refused before anything is sent, so the URL is never used.
        replay([transfer(STEP_1_MS)], "http://127.0.0.1:1", watermark)


def test_the_wait_returns_once_the_watermark_has_reached_the_time() -> None:
    clock = Clock()
    values = iter([5, 9, 10])

    class Rising:
        def read(self) -> int:
            return next(values)

    wait_for(Rising(), 10, clock=clock, sleep=clock.sleep)
    assert clock.now == pytest.approx(0.002)  # it polled twice before 10 arrived


def test_the_wait_gives_up_after_its_timeout() -> None:
    clock = Clock()
    with pytest.raises(ReplayError, match=f"within {WAIT_TIMEOUT_S} s"):
        wait_for(FixedWatermark(9), 10, clock=clock, sleep=clock.sleep)
    assert clock.now == pytest.approx(WAIT_TIMEOUT_S)
