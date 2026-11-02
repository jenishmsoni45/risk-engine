# Fixtures captured from real tally

Four events from tally's `ledger.entries`, as tally published them (D25), for
`tests/integration/test_contract_tally.py`:

| File | What | `riskCheck` |
|---|---|---|
| `transfer_decided.json` | a transfer the decision API allowed | `decided`, with its `decisionId` |
| `transfer_fallback.json` | a transfer posted by the backup answer `allow` | `fallback` |
| `entry.json` | a multi-line entry (the funding entry) | `none` |
| `reversal.json` | the reversal of the decided transfer | `none` |

Each file holds the message's topic, partition, key, headers (`[name, text]` pairs) and value, the
value exactly as received, as text. `scripts/capture_tally_fixtures.py` writes them.

## When and from what

Captured on 2026-11-02 from the owner's clone, `~/Desktop/tally`, at `0814db4`, whose tree is
`main`'s after tally PR #14 (Phase 10, the risk decision call). tally ran with `./gradlew bootRun`
and its own Compose stack; the decision API was this repo's, from this phase's branch.

The capture found that every tally event carries one header, `traceparent`, which design.md said
it didn't; D44 records it and what the stub does about it.

## How to capture them again

Run from this repo, with this repo's stack up (`make up`) and nothing else on port 8080.

1. Stop the decision API, so that tally's first transfer gets no answer:
   `docker compose --env-file .env.example stop api`
2. In tally's clone, start tally with the risk check on, pointed at this repo's API, and with
   `allow` as the backup answer before any reply. `--offline` downloads nothing and
   `--no-daemon` leaves no Gradle process behind:

   ```
   TALLY_RISK_ENABLED=true TALLY_RISK_URL=http://localhost:8000 TALLY_RISK_ONTIMEOUTDEFAULT=allow \
     ./gradlew --offline --no-daemon bootRun
   ```

3. Once `curl localhost:8080/actuator/health` says `UP`, create three CAD accounts: a funding
   account that may go negative, Alice and Bob (`POST /accounts` with `code`, `currency` and,
   for the funding account, `"allowNegative": true`).
4. The entry: `POST /entries` with an `Idempotency-Key`, moving 100000 from the funding account to
   Alice.
5. The fallback transfer: `POST /transfers` from Alice to Bob, 2500. The API is down and tally has
   had no reply yet, so tally posts it with its configured backup answer, `allow`.
6. Start the decision API again: `docker compose --env-file .env.example up --detach --wait api`
7. The decided transfer: `POST /transfers` from Alice to Bob, 1500. The API allows it.
8. The reversal: `POST /entries/<decided transfer id>/reverse` with a new `Idempotency-Key`.
9. Save the four events from tally's broker:

   ```
   uv run --locked python scripts/capture_tally_fixtures.py --bootstrap localhost:9092 \
     transfer_decided=<id> transfer_fallback=<id> entry=<id> reversal=<id>
   ```

10. Stop tally with Ctrl+C (or SIGTERM to its process); Spring Boot then stops tally's Compose
    stack.

tally's local database keeps the three accounts and four entries; they don't affect tally's tests,
which use their own databases.
