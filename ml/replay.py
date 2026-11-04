"""The replay (design.md section 8, D36): loaded PaySim transfers posted one at a time through the
ledger stub in replay mode, each decided by the decision API and applied by the consumer before
the next is asked about.

Before each request the replay waits until the watermark in Redis has reached the previous
transfer's time. Times strictly increase (D52e), and only one event is ever in flight, so by then
every earlier event has been applied. Each decision therefore sees every earlier transfer, as the
offline pipeline does, which is what lets I7 be exact rather than close.

The run stops at the first transfer that isn't posted at its replay time (D52h): any answer but
201, or a 201 whose createdAt isn't the replay time (the stub isn't in replay mode). It refuses to
start on a stack whose watermark is already at or past the first transfer, because then the wait
would never wait.

Run `python -m ml.replay --csv PATH` against a stack whose stub runs with STUB_REPLAY=1;
`make parity` does that on a throwaway stack.
"""

from __future__ import annotations

import argparse
import itertools
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Protocol

import httpx
import redis

from ml.paysim import CURRENCY, Transfer, load
from risk.features.redis_store import WATERMARK_KEY, parse_watermark
from risk.timestamps import EPOCH, epoch_ms, format_instant, parse_event_timestamp

SAMPLE_SIZE = 10_000  # D52c: the first 10,000 loaded transfers in time order
STUB_URL = "http://127.0.0.1:8001"
REDIS_URL = "redis://127.0.0.1:6379/0"
WAIT_TIMEOUT_S = 30.0  # D52h
WAIT_POLL_S = 0.001
REQUEST_TIMEOUT_S = 10.0


class ReplayError(Exception):
    """The replay stopped: a transfer wasn't posted at its time, or wasn't applied in time."""


class Watermark(Protocol):
    def read(self) -> int: ...


class RedisWatermark:
    """`wm` in the stack's Redis: the latest createdAt the consumer has applied (design.md 3.2)."""

    def __init__(self, url: str = REDIS_URL) -> None:
        self.client = redis.Redis.from_url(url, socket_timeout=5, socket_connect_timeout=5)

    def read(self) -> int:
        return parse_watermark(self.client.get(WATERMARK_KEY))

    def close(self) -> None:
        self.client.close()


@dataclass(frozen=True)
class ReplayResult:
    transfers: int
    elapsed_s: float


def replay_time(transfer: Transfer) -> str:
    return format_instant(EPOCH + timedelta(milliseconds=transfer.time_ms))


def request(transfer: Transfer) -> tuple[dict[str, str], dict[str, Any]]:
    """The headers and body of the transfer's POST /transfers."""
    headers = {"Idempotency-Key": transfer.request_id, "X-Replay-Time": replay_time(transfer)}
    body = {
        "fromAccountId": str(transfer.sender),
        "toAccountId": str(transfer.receiver),
        "amount": transfer.amount,
        "currency": CURRENCY,
    }
    return headers, body


def wait_for(
    watermark: Watermark,
    time_ms: int,
    timeout_s: float = WAIT_TIMEOUT_S,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Returns once the watermark has reached time_ms; raises ReplayError after timeout_s."""
    deadline = clock() + timeout_s
    while watermark.read() < time_ms:
        if clock() >= deadline:
            raise ReplayError(
                f"the consumer didn't apply the event at {time_ms} within {timeout_s} s"
            )
        sleep(WAIT_POLL_S)


def check_order(transfers: Sequence[Transfer]) -> None:
    for earlier, later in itertools.pairwise(transfers):
        if later.time_ms <= earlier.time_ms:
            raise ReplayError(
                f"line {later.line}: times must strictly increase for the wait to be exact (D52e)"
            )


def _posted_at(response: httpx.Response) -> int | None:
    try:
        return epoch_ms(parse_event_timestamp(response.json()["createdAt"]))
    except (ValueError, KeyError, TypeError):
        return None


def replay(
    transfers: Sequence[Transfer],
    stub_url: str,
    watermark: Watermark,
    *,
    wait_timeout_s: float = WAIT_TIMEOUT_S,
    progress: Callable[[int], None] | None = None,
) -> ReplayResult:
    """Posts every transfer in order, each after the previous one's event has been applied, and
    returns once the last one's has been too."""
    check_order(transfers)
    started = time.monotonic()
    if transfers and (applied := watermark.read()) >= transfers[0].time_ms:
        raise ReplayError(
            f"the watermark ({applied}) is already at or past the first transfer's time "
            f"({transfers[0].time_ms}): the replay needs a fresh stack (D52a)"
        )
    with httpx.Client(base_url=stub_url, timeout=REQUEST_TIMEOUT_S) as client:
        for number, transfer in enumerate(transfers):
            if number:
                wait_for(watermark, transfers[number - 1].time_ms, wait_timeout_s)
            headers, body = request(transfer)
            response = client.post("/transfers", headers=headers, json=body)
            if response.status_code != 201:
                raise ReplayError(
                    f"transfer {number} (line {transfer.line}) got {response.status_code}, not "
                    f"201: {response.text[:500]}"
                )
            if _posted_at(response) != transfer.time_ms:
                raise ReplayError(
                    f"transfer {number} (line {transfer.line}) wasn't posted at its replay time "
                    f"{replay_time(transfer)}: is the stub running with STUB_REPLAY=1?"
                )
            if progress is not None:
                progress(number + 1)
    if transfers:
        wait_for(watermark, transfers[-1].time_ms, wait_timeout_s)
    return ReplayResult(len(transfers), time.monotonic() - started)


def print_progress(total: int) -> Callable[[int], None]:
    def report(done: int) -> None:
        if done % 1000 == 0 or done == total:
            print(f"replayed {done} of {total}", flush=True)

    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--csv", type=Path, required=True, help="a PaySim CSV")
    parser.add_argument(
        "--count", type=int, default=SAMPLE_SIZE, help="the first COUNT transfers (D52c)"
    )
    parser.add_argument("--stub", default=STUB_URL, help="the stub's URL, in replay mode")
    parser.add_argument("--redis", default=REDIS_URL, help="the stack's Redis, for the watermark")
    args = parser.parse_args(argv)
    transfers = list(itertools.islice(load(args.csv), args.count))
    watermark = RedisWatermark(args.redis)
    try:
        result = replay(transfers, args.stub, watermark, progress=print_progress(len(transfers)))
    except ReplayError as exc:
        print(f"replay stopped: {exc}", file=sys.stderr)
        return 1
    finally:
        watermark.close()
    print(f"replayed {result.transfers} transfers in {result.elapsed_s:.1f} s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
