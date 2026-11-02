"""The feature consumer process (design.md 3.2).

Phase 1 runs the process and serves GET /metrics on CONSUMER_METRICS_PORT (8002, D40z).
Reading ledger.entries arrives in Phase 3.
"""

from __future__ import annotations

import logging
import os
import signal
import threading
from types import FrameType

from prometheus_client import start_http_server

log = logging.getLogger(__name__)


def run(port: int, stop: threading.Event) -> None:
    # Listens on every interface inside the container; Compose publishes it on 127.0.0.1 only.
    server, thread = start_http_server(port)
    log.info("consumer metrics on port %d", port)
    try:
        stop.wait()
    finally:
        server.shutdown()
        thread.join()
        log.info("consumer stopped")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    stop = threading.Event()

    def on_signal(signum: int, _frame: FrameType | None) -> None:
        log.info("received signal %d, stopping", signum)
        stop.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    run(int(os.environ.get("CONSUMER_METRICS_PORT", "8002")), stop)


if __name__ == "__main__":
    main()
