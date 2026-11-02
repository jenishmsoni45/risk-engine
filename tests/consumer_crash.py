"""The feature consumer for tests/adversarial/test_consumer_crash.py, run on this machine as
`python -m tests.consumer_crash`: the real loop, configured from the environment like the
service, which dies like a killed process right after its first batch's Redis writes and before
that batch's offsets are committed.
"""

from __future__ import annotations

import logging
import os
import threading

from confluent_kafka import TopicPartition

from risk.features.consumer import ConsumerConfig, run

CRASH_EXIT = 17


def crash(offsets: list[TopicPartition]) -> None:
    # os._exit skips every cleanup, as a kill does: no commit, no leaving the group.
    print(f"crashing before committing {[(o.partition, o.offset) for o in offsets]}", flush=True)  # noqa: T201
    os._exit(CRASH_EXIT)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run(
        ConsumerConfig.from_env(),
        threading.Event(),
        before_commit=crash,
        # The group drops a member that stops heartbeating after its session timeout, 45 s by
        # default; a short one lets the restarted consumer take the partitions within seconds.
        kafka_overrides={"session.timeout.ms": 6000, "heartbeat.interval.ms": 1000},
    )


if __name__ == "__main__":
    main()
