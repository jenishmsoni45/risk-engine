"""A fake decision API for the stub's tests, which answers, or misbehaves, as a test asks.

It speaks HTTP/1.1 over raw sockets, so it can do what a real server never would: accept and stay
silent, send headers and no body, answer late. It runs on its own event loop in a thread, on the
loopback only, and records every request body it receives.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import threading
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, Self

# Writes a response, or doesn't. Returns whether the connection may serve another request.
Responder = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[bool]]


def _response(status: int, body: bytes, content_type: str = "application/json") -> bytes:
    head = (
        f"HTTP/1.1 {status} X\r\nContent-Type: {content_type}\r\n"
        f"Content-Length: {len(body)}\r\n\r\n"
    )
    return head.encode() + body


def raw(status: int, body: bytes = b"") -> Responder:
    async def respond(_reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> bool:
        writer.write(_response(status, body))
        await writer.drain()
        return True

    return respond


def decided(
    decision: str = "allow",
    on_timeout: str = "hold",
    decision_id: str | None = None,
    **extra: Any,
) -> Responder:
    """A 200 that keeps the contract, with a new decisionId unless one is given."""
    body = {
        "decisionId": decision_id or str(uuid.uuid4()),
        "decision": decision,
        "ruleSet": "v1",
        "fired": [],
        "onTimeout": on_timeout,
        **extra,
    }
    return raw(200, json.dumps(body).encode())


def late(delay_s: float, then: Responder) -> Responder:
    async def respond(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> bool:
        await asyncio.sleep(delay_s)
        return await then(reader, writer)

    return respond


async def _until_closed(reader: asyncio.StreamReader) -> bool:
    """Waits for the client to give up and close the connection."""
    with contextlib.suppress(ConnectionError):
        while await reader.read(1024):
            pass
    return False


async def silent(reader: asyncio.StreamReader, _writer: asyncio.StreamWriter) -> bool:
    """Accepts the request and never answers."""
    return await _until_closed(reader)


async def headers_only(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> bool:
    """Sends a 200's headers promising a body, then nothing."""
    writer.write(
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 120\r\n\r\n"
    )
    await writer.drain()
    return await _until_closed(reader)


async def _read_request(reader: asyncio.StreamReader) -> bytes | None:
    try:
        head = await reader.readuntil(b"\r\n\r\n")
    except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, ConnectionError):
        return None
    length = 0
    for line in head.decode("latin-1").split("\r\n")[1:]:
        name, _, value = line.partition(":")
        if name.strip().lower() == "content-length":
            length = int(value.strip())
    try:
        return await reader.readexactly(length)
    except (asyncio.IncompleteReadError, ConnectionError):
        return None


class FakeRisk:
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.responder: Responder = decided()
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._server: asyncio.Server | None = None
        self.port = 0

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def calls(self) -> int:
        return len(self.requests)

    def respond_with(self, responder: Responder) -> None:
        self.responder = responder

    def __enter__(self) -> Self:
        self._thread.start()
        future = asyncio.run_coroutine_threadsafe(
            asyncio.start_server(self._handle, "127.0.0.1", 0), self._loop
        )
        self._server = future.result(timeout=5)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    def __exit__(self, *_exc: object) -> None:
        async def stop() -> None:
            if self._server is not None:
                self._server.close()
            tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        asyncio.run_coroutine_threadsafe(stop(), self._loop).result(timeout=5)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)
        self._loop.close()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while (body := await _read_request(reader)) is not None:
                self.requests.append(json.loads(body))
                if not await self.responder(reader, writer):
                    break
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
