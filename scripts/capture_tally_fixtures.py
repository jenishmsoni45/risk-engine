"""Saves ledger.entries events from a running tally as contract fixtures (D25).

    uv run --locked python scripts/capture_tally_fixtures.py transfer_decided=<entry id> \
        transfer_fallback=<entry id> entry=<entry id> reversal=<entry id>

Reads tally's broker (default localhost:9092, tally's host listener) from the start of every
partition and writes tests/fixtures/tally/<name>.json for each entry id: the message's key,
headers and value exactly as received (the value as text, never re-encoded). The steps that
produce the four entries are in tests/fixtures/tally/README.md.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition

TOPIC = "ledger.entries"
OUT = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "tally"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("events", nargs="+", metavar="NAME=ENTRY_ID")
    args = parser.parse_args()

    wanted: dict[str, str] = {}
    for item in args.events:
        name, _, entry_id = item.partition("=")
        wanted[str(uuid.UUID(entry_id))] = name

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": f"risk-engine-fixture-capture-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    partitions = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
    consumer.assign([TopicPartition(TOPIC, p, 0) for p in partitions])
    found: dict[str, dict[str, object]] = {}
    deadline = time.monotonic() + args.timeout
    try:
        while len(found) < len(wanted) and time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None or message.error() is not None:
                continue
            key = (message.key() or b"").decode()
            if key in wanted and wanted[key] not in found:
                value = message.value() or b""
                headers = message.headers()
                pairs = headers.items() if isinstance(headers, dict) else (headers or [])
                found[wanted[key]] = {
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "key": key,
                    # [name, value as text] pairs; null when the message has none.
                    "headers": None
                    if headers is None
                    else [
                        [name, raw.decode() if isinstance(raw, bytes) else (raw or "")]
                        for name, raw in pairs
                    ],
                    "value": value.decode(),
                }
    finally:
        consumer.close()

    missing = sorted(set(wanted.values()) - set(found))
    if missing:
        print(f"not found on {TOPIC} within {args.timeout} s: {missing}", file=sys.stderr)
        return 1
    OUT.mkdir(parents=True, exist_ok=True)
    for name, fixture in found.items():
        path = OUT / f"{name}.json"
        path.write_text(json.dumps(fixture, indent=2) + "\n")
        print(f"wrote {path.relative_to(OUT.parent.parent.parent)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
