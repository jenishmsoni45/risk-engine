"""I7 (design.md section 6): offline features equal the features logged online. Phase 6,
criterion 6, in CI on generated PaySim-format data (D37); `make parity` runs the same check on
about 10k real transfers (D36).

The generated transfers are replayed through a replay-mode stub, run in this process, against the
stack's decision API and consumer (D52b): every transfer is decided and posted, and each waits for
the previous one's event. Then every logged snapshot must equal the offline pipeline's recompute
over the same transfers, exactly, and every feature family and window must have held members for
some transfers, so the equality isn't between empty windows.
"""

from __future__ import annotations

from pathlib import Path

from ml.parity import check
from ml.replay import RedisWatermark, replay
from risk.features.compute import FAMILIES, WINDOWS
from tests.feature_state import STACK_DB, redis_url
from tests.helpers import API_URL, database
from tests.replay_setup import fresh_watermark, generated_transfers
from tests.stub_process import running_stub


def test_every_logged_snapshot_equals_the_offline_recompute(tmp_path: Path) -> None:
    transfers = generated_transfers(tmp_path)
    fresh_watermark()
    watermark = RedisWatermark(redis_url(STACK_DB))
    try:
        with running_stub(API_URL, replay=True) as stub:
            result = replay(transfers, stub, watermark)
    finally:
        watermark.close()
    assert result.transfers == len(transfers)

    with database() as conn:
        report = check(transfers, conn)
    assert report.missing == []
    assert report.mismatches == []
    assert report.equal == report.compared == len(transfers)
    for family in FAMILIES:
        for window, _ in WINDOWS:
            assert report.nonzero.get(f"{family}_count_{window}", 0) > 0, (family, window)
