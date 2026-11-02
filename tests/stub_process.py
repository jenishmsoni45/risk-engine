"""Runs the ledger stub in the test process, pointed at a fake decision API.

The stub's code is the container's; only its settings differ. It uses the stack's Postgres and
Kafka through their host ports, publishes at once (relay interval 0), and starts with a fresh
memory of the backup answer, so each test controls what the stub has heard.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager

import uvicorn

from ledger_stub.app import create_app
from ledger_stub.config import StubConfig
from tests.helpers import KAFKA_HOST_BOOTSTRAP, free_port, postgres_conninfo


@contextmanager
def running_stub(
    risk_url: str,
    *,
    database_url: str | None = None,
    on_timeout_default: str = "hold",
) -> Iterator[str]:
    """The base URL of a stub that runs until the block ends."""
    config = StubConfig(
        database_url=database_url or postgres_conninfo(),
        kafka_bootstrap=KAFKA_HOST_BOOTSTRAP,
        risk_url=risk_url,
        risk_timeout_s=0.1,
        on_timeout_default=on_timeout_default,
        relay_poll_s=0,
        relay_batch=100,
    )
    port = free_port()
    server = uvicorn.Server(
        uvicorn.Config(create_app(config), host="127.0.0.1", port=port, log_level="warning")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 15
    while not server.started:
        assert thread.is_alive(), "the stub stopped while starting"
        assert time.monotonic() < deadline, "the stub didn't start within 15 s"
        time.sleep(0.02)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=15)
