# Postmortem: Kafka forgot its log while Postgres remembered

One real bug found while building 2a (D48.4). It never reached CI, which is why it is worth telling.

## Summary

The local stack kept Postgres's data in a named volume but kept Kafka's log inside the broker's container. `make down` removes containers, so every `make down` and `make up` started `ledger.entries` again at offset 0 while Postgres kept every row. Two things depended on the two lasting equally long:

- `bad_events` names each malformed message by its position, unique on `(topic, kafka_partition, kafka_offset)`, and the consumer inserts with `on conflict do nothing`, so that a message read again after a crash is stored once (D40i). After a reset, a new malformed message at an offset an earlier log had used was taken as already stored and silently skipped.
- `make rebuild-features` (design.md 3.5) refills Redis by replaying `ledger.entries`. After a reset there was nothing to replay, while the decision log still held the attempts of the old run.

The fix keeps Kafka's log in a named volume, `kafka-data`, beside `postgres-data`, so the two last exactly as long as each other, and `make clean` removes both (D41.6).

## Impact

Local only, and no wrong decision was ever served: the service had no real traffic. Had it shipped this way, a broker recreated under a database that survived would have lost malformed messages from the audit table without a trace, and the rebuild procedure would have had no input, which defeats design.md 3.5's claim that Redis holds nothing that can't be rebuilt.

## Timeline

1. **Phase 3.** `bad_events` and its unique position arrive with the consumer, and `tests/adversarial/test_bad_events.py` checks one row per malformed message. CI and the local run pass.
2. **Phase 4.** `make test` on this Mac fails several cases of `test_bad_events.py`; the same commit passes in CI. Nothing in Phase 4 touched the consumer.
3. **Diagnosis.** The missing rows were at offsets that `bad_events` already held from earlier local runs. The broker's log had restarted at 0 and Postgres's table had not. CI starts every service from nothing on every run, so it can never see two stores out of step.
4. **Decision (D47).** Wipe the local data so Phase 4 stays about rules; record the gap as a follow-up.
5. **Phase 5 (D48.4).** Fixed in commit `0daca66`, with a test, before the rebuild and fault tests that depend on it.

## Root cause

Two stores whose contents refer to each other had different lifetimes, and nothing said so. A Kafka offset is only a name for a message while the log that assigned it exists. Postgres outlived the log, so its rows went on naming positions that now held different messages, or none.

The idempotent insert, which is correct on its own, turned the mismatch from an error into silence: a conflict on the position looks exactly like a message read twice.

## Why it was missed

- CI starts empty, and the bug lives only in state carried from one run to the next.
- Each test made its own fresh accounts and ids, so tests never collided with each other's rows, only with old rows at the same offsets.
- Compose's defaults hid it: Postgres had a volume because databases obviously get one; Kafka's log looked like plumbing.

## Fix

- `docker-compose.yml` sets `KAFKA_LOG_DIRS` to a path in the named volume `kafka-data`, so the log, the consumer group's committed offsets and `bad_events` all survive `make down` and all go together on `make clean`.
- `tests/integration/test_kafka_log.py` recreates the broker's container, as `make down` and `make up` do, and checks that an event published before is still at its offset, that new events get later offsets, and that a malformed message published afterwards is stored under its own offset.

## Epilogue: the fix's own changeover

The bug struck once more, on the machine where it was fixed. The stack running there had been started before the fix, so its log still lived in its container. The first `make down` and `make up` afterwards created the new, empty `kafka-data` volume beside Postgres's old rows: the same mismatch, one last time. `test_bad_events.py` runs early in `make test`, while the new log's offsets were still low. Its cases published at offsets the old rows held, and they failed the way they did in Phase 4. So did the paused-Postgres fault test, which checks a bad event's row. Once the log had passed the old rows' highest offsets, the same tests passed with no data touched. From then on the two volumes last equally long, so it can't happen again.

## What changes from here

- Any state keyed by another store's positions shares that store's lifetime, or carries something that tells generations apart. This project takes the first option.
- A test that only ever runs on a fresh stack can't find a lifetime bug. The test above recreates one container on a stack that is already running instead.
- When an idempotent write skips a conflict, it is worth asking what else could produce the same conflict.
- Changing where a store keeps its data is itself a migration. The first start after the change pairs a new, empty store with the other stores' old state, so the change should clear that state too, or say plainly that the first run will fail this way.
