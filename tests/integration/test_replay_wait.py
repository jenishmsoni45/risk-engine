"""The replay's wait (design.md section 8, D36). Phase 6, criterion 4: with a relay delay above 0,
the replay doesn't send the next request until the watermark has reached the previous transfer's
time.

A replay-mode stub runs in this process with tally's relay interval of 200 ms (D40k), against the
stack's decision API and consumer, on a fresh watermark (D52b). Every decision row stores its lag,
t minus the watermark the decision read (design.md 3.4), so `requested_at - feature_lag_ms` is the
watermark each decision saw: with the wait it is never behind the previous transfer's time. The
control posts the same kind of transfers without waiting, and the relay's delay shows up as
decisions that saw an older watermark, so the check can tell the difference.
"""

from __future__ import annotations

from pathlib import Path

import httpx

from ml.paysim import Transfer
from ml.replay import RedisWatermark, replay, request, wait_for
from risk.timestamps import epoch_ms
from tests.feature_state import STACK_DB, redis_url
from tests.helpers import API_URL, database
from tests.replay_setup import fresh_watermark, generated_transfers
from tests.stub_process import running_stub

RELAY_POLL_S = 0.2  # tally's (D40k)
TRANSFERS = 15


def watermarks_seen(transfers: list[Transfer]) -> list[int]:
    """The watermark each transfer's decision read, in the transfers' order."""
    with database() as conn:
        rows = conn.execute(
            "select request_id, requested_at, feature_lag_ms from decisions "
            "where request_id = any(%s)",
            ([t.request_id for t in transfers],),
        ).fetchall()
    seen = {request_id: epoch_ms(at) - lag for request_id, at, lag in rows}
    return [seen[t.request_id] for t in transfers]


def behind_previous(transfers: list[Transfer], seen: list[int]) -> list[int]:
    """The transfers whose decision read a watermark behind the previous transfer's time."""
    return [
        number
        for number in range(1, len(transfers))
        if seen[number] < transfers[number - 1].time_ms
    ]


def test_each_request_waits_for_the_previous_transfers_event(tmp_path: Path) -> None:
    transfers = generated_transfers(tmp_path)[:TRANSFERS]
    fresh_watermark()
    watermark = RedisWatermark(redis_url(STACK_DB))
    try:
        with running_stub(API_URL, replay=True, relay_poll_s=RELAY_POLL_S) as stub:
            replay(transfers, stub, watermark)
    finally:
        watermark.close()
    assert behind_previous(transfers, watermarks_seen(transfers)) == []


def test_without_the_wait_the_relays_delay_shows(tmp_path: Path) -> None:
    transfers = generated_transfers(tmp_path)[:TRANSFERS]
    fresh_watermark()
    watermark = RedisWatermark(redis_url(STACK_DB))
    try:
        with running_stub(API_URL, replay=True, relay_poll_s=RELAY_POLL_S) as stub:
            for transfer in transfers:
                headers, body = request(transfer)
                response = httpx.post(f"{stub}/transfers", headers=headers, json=body, timeout=10)
                assert response.status_code == 201, response.text
            # Every event is applied before the stub stops, so none is left for a later test.
            wait_for(watermark, transfers[-1].time_ms)
    finally:
        watermark.close()
    assert behind_previous(transfers, watermarks_seen(transfers)) != []
