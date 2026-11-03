"""`make e2e`: real tally against this service (D26, D48.6, Phase 5 criterion 6). Local only.

1. Checks that nothing else holds the ports: the normal stack must be stopped (`make down`), and
   tally must not be running already.
2. Starts tally from the owner's clone (TALLY_DIR, default ~/Desktop/tally) with the risk check on
   and pointed at this service: `./gradlew --offline --no-daemon bootRun`, which also starts
   tally's own Compose stack. `--offline` downloads nothing, and `--no-daemon` leaves no Gradle
   process behind.
3. Starts this repo's stack as the throwaway project `risk-engine-e2e`, whose consumer reads
   tally's broker in a new consumer group that starts at the end of tally's topic, and waits
   until that group has committed its starting offsets.
4. Through tally: funds an account, makes three transfers the starter rules allow, one they hold
   (`large_transfer`), and, with the decision API stopped, one that gets the backup answer.
5. Checks that every event tally published with `riskCheck: decided` names a decisionId in this
   service's `decisions`, that the consumer applied tally's events, that the held transfer got
   tally's 403 `transfer-held`, and that with the API stopped tally answered 503
   `risk-unavailable`.
6. Always: stops tally (SIGTERM to its process group; Spring Boot then stops tally's Compose
   stack) and removes the throwaway project with its volumes.

Never changes anything in tally's repo; tally's local database keeps the accounts and entries made
here, as after any of tally's own runs.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import psycopg
import redis
from confluent_kafka import Consumer, TopicPartition

ROOT = Path(__file__).resolve().parent.parent
TOPIC = "ledger.entries"
PROJECT = "risk-engine-e2e"
TALLY_URL = "http://localhost:8080"
TALLY_KAFKA = "localhost:9092"  # tally's host listener
REDIS_URL = "redis://127.0.0.1:6379/0"
POSTGRES_PORT = 5433
HELD = "/problems/transfer-held"
UNAVAILABLE = "/problems/risk-unavailable"
LARGE = 1_000_000  # D20: large_transfer holds an amount of 1,000,000 or more


class E2EError(Exception):
    pass


def env_file() -> Path:
    local = ROOT / ".env"
    return local if local.exists() else ROOT / ".env.example"


def read_env() -> dict[str, str]:
    values = {}
    for line in env_file().read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


def say(text: str) -> None:
    print(text, flush=True)


def check(ok: bool, what: str) -> None:
    say(f"{'ok  ' if ok else 'FAIL'} {what}")
    if not ok:
        raise E2EError(what)


def wait_for(condition: Callable[[], bool], what: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            if condition():
                return
        except (httpx.HTTPError, OSError):
            pass
        if time.monotonic() > deadline:
            raise E2EError(f"{what}: not within {timeout:.0f} s")
        time.sleep(1)


def port_in_use(port: int) -> bool:
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def compose(*args: str, project: str = PROJECT, env: dict[str, str] | None = None) -> str:
    files = ["-f", "docker-compose.yml", "-f", "docker-compose.e2e.yml"]
    if project != PROJECT:
        files = []
    done = subprocess.run(
        [
            "docker",
            "compose",
            "--project-name",
            project,
            "--env-file",
            str(env_file()),
            *files,
            *args,
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
        env={**os.environ, **(env or {})},
    )
    if done.returncode != 0:
        raise E2EError(f"docker compose {' '.join(args)} failed:\n{done.stdout}\n{done.stderr}")
    return done.stdout


# --- tally ---------------------------------------------------------------------------------------


class Tally:
    def __init__(self, folder: Path) -> None:
        self.folder = folder
        self.log = Path(tempfile.mkdtemp(prefix="risk-engine-e2e-")) / "tally.log"
        self.process: subprocess.Popen[bytes] | None = None

    def start(self) -> None:
        env = {
            **os.environ,
            "TALLY_RISK_ENABLED": "true",
            "TALLY_RISK_URL": "http://localhost:8000",
        }
        with self.log.open("wb") as out:
            process = self.process = subprocess.Popen(
                ["./gradlew", "--offline", "--no-daemon", "bootRun"],
                cwd=self.folder,
                env=env,
                stdout=out,
                stderr=subprocess.STDOUT,
                # Its own process group, so one signal reaches Gradle and tally's JVM.
                start_new_session=True,
            )
        say(f"tally starting from {self.folder}; its output goes to {self.log}")

        def up() -> bool:
            if process.poll() is not None:
                raise E2EError(f"tally exited with {process.returncode}; see {self.log}")
            return bool(
                httpx.get(f"{TALLY_URL}/actuator/health", timeout=2).json()["status"] == "UP"
            )

        wait_for(up, "tally healthy", timeout=600)

    def stop(self) -> None:
        if self.process is None or self.process.poll() is not None:
            return
        os.killpg(self.process.pid, signal.SIGTERM)
        try:
            self.process.wait(timeout=120)
        except subprocess.TimeoutExpired:
            os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait(timeout=30)
        say(f"tally stopped (exit {self.process.returncode})")

    def post(self, path: str, body: dict[str, Any], key: str | None = None) -> httpx.Response:
        headers = {"Idempotency-Key": key} if key else {}
        return httpx.post(f"{TALLY_URL}{path}", json=body, headers=headers, timeout=10)


def git(folder: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(folder), *args], capture_output=True, text=True, check=False
    ).stdout.strip()


# --- Kafka ---------------------------------------------------------------------------------------


def kafka_consumer(bootstrap: str, group: str) -> Consumer:
    return Consumer(
        {"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False}
    )


def ends(bootstrap: str) -> dict[int, int]:
    consumer = kafka_consumer(bootstrap, f"e2e-ends-{uuid.uuid4().hex}")
    try:
        partitions = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
        return {
            p: consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)[1]
            for p in partitions
        }
    finally:
        consumer.close()


def committed(bootstrap: str, group: str, partitions: list[int]) -> dict[int, int]:
    consumer = kafka_consumer(bootstrap, group)
    try:
        offsets = consumer.committed([TopicPartition(TOPIC, p) for p in partitions], timeout=10)
        return {tp.partition: tp.offset for tp in offsets}
    finally:
        consumer.close()


def events_since(bootstrap: str, start: dict[int, int], wanted: set[str]) -> dict[str, Any]:
    """Every event on tally's topic from the given offsets, by entry id, waiting until the wanted
    ones have all arrived (tally's relay publishes every 200 ms)."""
    consumer = kafka_consumer(bootstrap, f"e2e-read-{uuid.uuid4().hex}")
    found: dict[str, Any] = {}
    try:
        consumer.assign([TopicPartition(TOPIC, p, o) for p, o in start.items()])
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is not None and message.error() is None:
                value = message.value()
                if value is not None:
                    document = json.loads(value)
                    found[document["id"]] = document
            elif message is None and wanted <= set(found):
                break
    finally:
        consumer.close()
    return found


def has_member(client: redis.Redis, name: str, member: str) -> Callable[[], bool]:
    return lambda: client.zscore(name, member) is not None


# --- The run -------------------------------------------------------------------------------------


def scenario(tally: Tally) -> None:
    run = uuid.uuid4().hex[:8]
    group = f"risk-features-e2e-{run}"
    tally_start = ends(TALLY_KAFKA)

    say(f"starting this repo's stack as {PROJECT}, consumer group {group} at tally's end")
    compose("down", "--volumes", "--remove-orphans", env={"E2E_CONSUMER_GROUP": group})
    compose(
        "up", "--detach", "--build", "--wait", "--wait-timeout", "300",
        env={"E2E_CONSUMER_GROUP": group},
    )  # fmt: skip
    partitions = sorted(tally_start)
    wait_for(
        lambda: all(o >= 0 for o in committed(TALLY_KAFKA, group, partitions).values()),
        "the consumer committed its starting offsets on tally's topic",
        timeout=120,
    )
    check(True, "the consumer is reading tally's topic from its end")

    def account(code: str, allow_negative: bool) -> str:
        response = tally.post(
            "/accounts",
            {"code": f"{code}-{run}", "currency": "CAD", "allowNegative": allow_negative},
        )
        check(response.status_code == 201, f"tally created account {code}: {response.status_code}")
        return str(response.json()["id"])

    funding, alice, bob = account("funding", True), account("alice", False), account("bob", False)

    allowed: list[dict[str, Any]] = []
    for sender, receiver, amount in (
        (funding, alice, 50_000),
        (alice, bob, 1_000),
        (alice, bob, 1_100),
        (alice, bob, 1_200),
    ):
        key = f"e2e-{run}-{len(allowed)}"
        body = {"fromAccountId": sender, "toAccountId": receiver, "amount": amount}
        response = tally.post("/transfers", {**body, "currency": "CAD"}, key)
        entry = response.json()
        check(
            response.status_code == 201 and entry.get("riskCheck") == "decided",
            f"tally posted {amount} with riskCheck decided: {response.status_code}",
        )
        allowed.append({**entry, "key": key})

    held_key = f"e2e-{run}-held"
    held = tally.post(
        "/transfers",
        {"fromAccountId": funding, "toAccountId": bob, "amount": LARGE, "currency": "CAD"},
        held_key,
    )
    check(
        held.status_code == 403 and held.json().get("type") == HELD,
        f"a transfer of {LARGE} got tally's 403 transfer-held: {held.status_code}",
    )
    held_decision = held.json().get("decisionId")

    compose("stop", "api", env={"E2E_CONSUMER_GROUP": group})
    unavailable = tally.post(
        "/transfers",
        {"fromAccountId": funding, "toAccountId": bob, "amount": 700, "currency": "CAD"},
        f"e2e-{run}-unavailable",
    )
    check(
        unavailable.status_code == 503 and unavailable.json().get("type") == UNAVAILABLE,
        f"with the API stopped tally answered 503 risk-unavailable: {unavailable.status_code}",
    )

    # The join: every decided event on tally's topic names a decision in this service's log.
    events = events_since(TALLY_KAFKA, tally_start, {e["id"] for e in allowed})
    decided = [e for e in events.values() if e.get("riskCheck") == "decided"]
    values = read_env()
    with psycopg.connect(
        f"host=127.0.0.1 port={POSTGRES_PORT} user={values['POSTGRES_USER']} "
        f"password={values['POSTGRES_PASSWORD']} dbname={values['POSTGRES_DB']}",
        autocommit=True,
    ) as conn:
        rows = {
            str(decision_id): (request_id, decision, fired)
            for decision_id, request_id, decision, fired in conn.execute(
                "select decision_id, request_id, decision, fired from decisions"
            )
        }
    check(
        {e["id"] for e in allowed} <= set(events),
        f"tally published all {len(allowed)} allowed transfers ({len(events)} events read)",
    )
    for event in decided:
        check(
            event["decisionId"] in rows and rows[event["decisionId"]][1] == "allow",
            f"entry {event['id']}'s decisionId {event['decisionId']} is an allow in decisions",
        )
    for entry in allowed:
        check(
            rows.get(entry["decisionId"], ("",))[0] == entry["key"],
            f"decision {entry['decisionId']} was asked with the transfer's Idempotency-Key",
        )
    check(
        held_decision in rows and rows[held_decision][1:] == ("hold", ["large_transfer"]),
        f"the held transfer's decision {held_decision} is a hold by large_transfer",
    )
    check(
        not any(e.get("decisionId") == held_decision for e in events.values()),
        "the held transfer published no event",
    )

    # The consumer applied tally's events: each allowed transfer is a member of its sender's set.
    client = redis.Redis.from_url(REDIS_URL, socket_timeout=5)
    try:
        for entry in allowed:
            sender = next(line for line in entry["lines"] if line["amount"] < 0)
            name = f"f:{sender['accountId']}:out"
            member = f"{-sender['amount']}:{entry['id']}"
            wait_for(
                has_member(client, name, member),
                f"the consumer applied entry {entry['id']}",
                timeout=60,
            )
            check(True, f"the consumer applied entry {entry['id']} to {name}")
    finally:
        client.close()


def main() -> int:
    folder = Path(os.environ.get("TALLY_DIR") or Path.home() / "Desktop" / "tally").expanduser()
    if not (folder / "gradlew").exists():
        say(f"no tally clone at {folder}: set TALLY_DIR")
        return 2
    if compose("ps", "--quiet", project="risk-engine").strip():
        say("the normal stack is running and uses the same ports: run `make down` first")
        return 2
    busy = [port for port in (8000, 8080, 9092, 9094) if port_in_use(port)]
    if busy:
        say(f"ports {busy} are in use: stop tally or whatever holds them first")
        return 2
    say(
        f"tally at {git(folder, 'rev-parse', '--short', 'HEAD')} on "
        f"{git(folder, 'branch', '--show-current') or 'a detached HEAD'}, "
        f"{'clean' if not git(folder, 'status', '--porcelain') else 'with local changes'}"
    )
    tally = Tally(folder)
    try:
        tally.start()
        scenario(tally)
    except E2EError as exc:
        say(f"e2e FAILED: {exc}")
        return 1
    finally:
        tally.stop()
        try:
            compose("down", "--volumes", "--remove-orphans", env={"E2E_CONSUMER_GROUP": "down"})
            say(f"{PROJECT} removed")
        except E2EError as exc:
            say(f"could not remove {PROJECT}: {exc}")
    say("e2e passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
