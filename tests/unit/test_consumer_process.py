"""The consumer process serves /metrics and stops cleanly on SIGTERM (as `make down` sends)."""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time

import httpx

from tests.helpers import ROOT


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port


def test_serves_metrics_then_stops_on_sigterm() -> None:
    port = free_port()
    env = {**os.environ, "CONSUMER_METRICS_PORT": str(port)}
    process = subprocess.Popen(
        [sys.executable, "-m", "risk.features.consumer"],
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 15
        while True:
            try:
                response = httpx.get(f"http://127.0.0.1:{port}/metrics", timeout=1)
                break
            except httpx.TransportError:
                assert process.poll() is None, "the consumer exited before serving /metrics"
                assert time.monotonic() < deadline, "the consumer never served /metrics"
                time.sleep(0.1)
        assert response.status_code == 200
        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=5) == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        if process.stderr is not None:
            process.stderr.close()
