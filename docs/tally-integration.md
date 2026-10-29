# tally integration: the pre-commit risk decision call

The spec for the tally side of the risk engine's decision call. The decisions behind it are D2 to D11 in [decisions.md](decisions.md). Under D12 it was copied into tally as `docs/risk-decision-call.md`, with section 6 added to tally's PLAN.md as Phase 10 (branch `docs-phase-10-plan`). tally builds from its copy; any change to the contract is made in both. tally built it in its Phase 10 (PR #14, merged 2026-10-28); the refinements tally made to the contract on the way are folded in below and recorded as D13.

## 1. The story

Today a transfer goes: validate, open the posting transaction, move the money, write the outbox row, commit, answer 201. The relay later announces the entry on Kafka.

After this phase, when the risk check is switched on, tally first asks the risk engine ("the guard") whether the transfer may go ahead, and only then opens its transaction:

1. Validate the request, as today.
2. If this Idempotency-Key already has a saved response, skip the guard and replay (or 409), as today.
3. Ask the guard, waiting at most 100 ms. No transaction is open and no database connection is held while waiting.
4. Allow: post as today, and record the decision id on the entry. Hold or block: refuse with 403. The guard doesn't answer in time: use the backup answer.
5. The entry, its 201 body and its Kafka event carry `decisionId` and `riskCheck`, so the risk engine can join each decision to the money it moved.

## 2. Decisions this implements

| Decision | Summary |
|---|---|
| D2 | Real tally calls the decision API, built as a tally phase from this spec |
| D3 | Hold and block refuse the transfer with 403 and distinct problem types; the body has the `decisionId`, never the fired rules |
| D4 | tally waits at most 100 ms for the guard |
| D5, D10 | The backup answer comes from the guard's latest reply (`onTimeout`); before the first reply tally uses its config default, `hold` |
| D6 | A refusal by the backup answer is a 503 (nothing was decided; retry with the same key) |
| D7 | tally stores `decisionId` and `riskCheck` on the entry and sends both in the 201 and the event |
| D8 | Only `POST /transfers` asks the guard |
| D9 | The check is off by default |
| D11 | The guard's 409 (requestId reused for a different transfer) becomes tally's own key-conflict 409 |

## 3. The contract

### 3.1 tally → risk engine

```
POST {tally.risk.url}/v1/decisions
Content-Type: application/json

{
  "requestId": "<the transfer's Idempotency-Key>",
  "fromAccountId": "<uuid>",
  "toAccountId": "<uuid>",
  "amount": 12500,
  "currency": "CAD",
  "requestedAt": "2026-11-20T15:04:05.123Z"
}
```

- `requestId` is the Idempotency-Key exactly as received. `amount` is the positive amount in minor units, `currency` the 3-letter code, both as in the transfer request.
- `requestedAt` is tally's clock (`Instant.now()`) just before the call, ISO-8601 UTC. It is the decision time the risk engine computes features at.

### 3.2 risk engine → tally

| Guard response | Meaning | What tally does |
|---|---|---|
| 200 with a valid body, `decision: "allow"` | Allowed | Posts the transfer with `riskCheck: decided` and the `decisionId` |
| 200, `decision: "hold"` | Held for review | 403 problem, type `transfer-held`, with `decisionId`; nothing posts |
| 200, `decision: "block"` | Blocked | 403 problem, type `transfer-blocked`, with `decisionId`; nothing posts |
| 409 problem | This requestId was already decided for a different transfer | tally's existing key-conflict 409 ("Idempotency-Key was already used for a different request") |
| No answer within the timeout, connection refused or reset, any other status, a body over 16 KiB, a body that doesn't parse, or a body with a value outside the lists below | No decision | Backup answer (3.4) |

A 200 body:

```json
{
  "decisionId": "7c1e9a52-3a0f-4b8e-9d55-0f3c2b1a6e47",
  "decision": "hold",
  "ruleSet": "v3",
  "fired": ["velocity_1m"],
  "onTimeout": "hold"
}
```

- `decisionId` is a UUID in the canonical 8-4-4-4-12 form. `decision` and `onTimeout` are each one of `allow`, `hold`, `block`, in exact lowercase. tally treats a 200 that breaks any of these as no decision.
- A 200 body is at most 16 KiB; a longer one is no decision.
- tally ignores fields it doesn't know, reads no body on a 409, and doesn't follow redirects. Its request has a `Content-Length` (not chunked), uses HTTP/1.1 without compression, and carries a W3C `traceparent` header.
- tally uses `decisionId`, `decision` and `onTimeout`. It does not store, log or return `ruleSet` or `fired`; they stay in the risk engine's decision log.
- The same `requestId` with the same `fromAccountId`, `toAccountId`, `amount` and `currency` always gets the same `decisionId`, `decision`, `ruleSet` and `fired` back, whenever it is asked (the risk engine's invariant I1). `requestedAt` is not part of that match.
- `onTimeout` is always the current rule set's, in a replayed decision too, so a retried old key can't bring back an older rule set's backup answer (3.4).

### 3.3 tally → customer

Unchanged: every existing status and body, including 400 for validation, which runs before the guard is asked.

New:

```
HTTP/1.1 403 Forbidden
Content-Type: application/problem+json

{ "type": "/problems/transfer-held", "title": "Transfer held for review", "status": 403,
  "detail": "The risk check held this transfer for review.", "decisionId": "7c1e9a52-…" }
```

```
HTTP/1.1 403 Forbidden
Content-Type: application/problem+json

{ "type": "/problems/transfer-blocked", "title": "Transfer blocked", "status": 403,
  "detail": "The risk check blocked this transfer.", "decisionId": "7c1e9a52-…" }
```

```
HTTP/1.1 503 Service Unavailable
Content-Type: application/problem+json

{ "type": "/problems/risk-unavailable", "title": "Risk check unavailable", "status": 503,
  "detail": "The risk check did not answer in time; try again. A transfer retried with the same Idempotency-Key moves money at most once." }
```

- RFC 9457 allows a relative `type`, and tally kept the relative URIs above. tally's existing problems keep `about:blank`. The risk engine only needs the three types to be distinct and stable.
- A refused transfer (403 or 503) saves no idempotency key, like every other refusal in tally. A retry with the same key goes through the guard again: a held key is held again (I1), and a key refused by the backup answer gets a real decision once the guard is back.

### 3.4 The backup answer

- tally keeps the `onTimeout` of the latest valid 200 in memory. Before the first one, and after a restart, it uses `tally.risk.on-timeout-default` (default `hold`).
- Every 200 carries the current rule set's `onTimeout` (3.2), so the latest reply names the backup answer in force. Two replies in flight while the rule set changes can arrive in either order, so for a moment tally can keep the older rule set's value; the next reply puts it right.
- Backup `allow`: post the transfer with `riskCheck: fallback` and `decisionId: null`.
- Backup `hold` or `block`: 503 `risk-unavailable` (3.3). Nothing posts.

### 3.5 The entry and the event

Two new fields on every entry, in the 201 body, in replays saved after this phase, and in the Kafka event value:

```json
{
  "id": "…", "kind": "transfer", "reversesId": null, "description": "rent",
  "effectiveAt": "2026-11-20T15:04:05.123456Z", "createdAt": "2026-11-20T15:04:05.123456Z",
  "decisionId": "7c1e9a52-3a0f-4b8e-9d55-0f3c2b1a6e47",
  "riskCheck": "decided",
  "lines": [ … ]
}
```

| `riskCheck` | When | `decisionId` |
|---|---|---|
| `decided` | The guard answered allow | the guard's id |
| `fallback` | The guard didn't answer and the backup answer was allow | null |
| `none` | The check is off, or the entry isn't a transfer (entries, reversals) | null |

- Both fields are always present (null is sent, like `reversesId`).
- A reversal of a checked transfer is `none`: reversals never ask the guard.
- A replay of a response saved before tally's Phase 10 shows `decisionId: null` and `riskCheck: none`, which is V7's column default for that entry. Outbox rows written before Phase 10 don't have the fields, and the risk engine reads a missing field in an event as `decisionId: null, riskCheck: none`.
- tally's `docs/design.md` ("Event ordering and delivery") lists the two fields. Everything else about the event is unchanged: same topic, key, partitions, at-least-once delivery and line-id ordering.

## 4. Behaviour in detail

### 4.1 Order of steps for `POST /transfers` with the check on

1. Bean Validation, as today (400).
2. Replay check, outside any transaction: a read of the saved idempotency response for this key. If there is one, call the existing transactional `postTransfer`, which replays it or answers 409, exactly as today. The guard is not asked.
3. Ask the guard (no transaction open, no pooled connection held).
4. Act on the answer (3.2, 3.4). For allow or backup allow, call the transactional posting path, which now also writes `decision_id` and `risk_check` on the entry.

The transaction boundary stays on the posting service, so the guard call must happen in a different bean: a non-transactional caller (for example a small transfer orchestrator in the posting package, or the controller) runs steps 2 to 4. A call from inside the `@Transactional` method would hold the transaction open while waiting, which is what this design avoids.

### 4.2 Races and edge cases

- Two first requests with one key at the same moment: both pass step 2 and both ask the guard, which gives both the same answer (I1). Both then reach the idempotency claim; one posts and the other replays, as today.
- A key saved between step 2 and step 4: the claim finds it and replays. The guard's second decision for that key is the same decision.
- The guard is asked before tally's own ledger checks (unknown account 404, wrong currency or insufficient funds 422), so it also sees transfers tally then refuses. tally does not tell the guard about that outcome.
- A transfer the guard allowed can still be refused by tally (for example insufficient funds). It then has a decision but no entry. That is expected.

### 4.3 The timeout

- The whole guard call (connect, send, wait, read the body) ends within `tally.risk.timeout` plus a small margin, whatever the guard does: accept the connection and never answer, answer slowly, or send a body slowly. The tests measure this (section 6, test 4).
- Connections to the guard are reused (keep-alive), so a normal call doesn't pay for a new TCP connection. tally closes a connection after 4 s idle, and the risk engine keeps an idle connection open for at least 5 s, so tally never sends a request on a connection the risk engine is closing. The JDK client doesn't retry a POST, so such a request would be no decision.
- At most 100 calls are in flight per tally process. A transfer that finds 100 in flight doesn't wait for a slot: it is no decision at once (outcome `unavailable`) and gets the backup answer.
- tally uses Spring's JDK request factory with one deadline for the whole call; its `RiskDecisionIT` measures the bound.

### 4.4 Configuration

```yaml
tally:
  risk:
    # Off by default (D9): tally behaves exactly as before unless this is switched on.
    enabled: ${TALLY_RISK_ENABLED:false}
    # The risk engine's decision API. 8000 is a placeholder until the risk engine's Phase 1 fixes its port;
    # it must not be one of tally's ports (8080, 5432, 9092, 9094, 3000, 4317, 4318).
    url: ${TALLY_RISK_URL:http://localhost:8000}
    # D4: the longest tally waits for a decision before using the backup answer.
    timeout: 100ms
    # D5, D10: the backup answer until the guard's first reply names one.
    on-timeout-default: hold
```

At startup, with `enabled: true`: `url` must be an absolute http(s) URL, `timeout` must be at least 1 ms, and `on-timeout-default` must be `allow`, `hold` or `block`, or tally refuses to start.

### 4.5 Observability

- Timer `ledger.risk.duration` around the guard call, tagged `outcome` = `allow`, `hold`, `block`, `conflict`, `timeout`, `unavailable` (connection refused or reset, a status other than 200 and 409, or 100 calls already in flight) or `invalid` (a body that breaks 3.2, one over 16 KiB included), with histogram buckets for p95 and p99 like tally's other timers.
- The call is made through a client built from Spring Boot's auto-configured builder, so it gets a client span and sends the W3C `traceparent` header. The risk engine can then continue the same trace.
- `decisionId` goes into the log context next to `entry_id` and the idempotency key on the "Posted entry" line. A backup answer logs one WARN line with the key, the outcome tag and the backup answer used. No request or response bodies are logged.
- `ledger.posting.duration` keeps timing the whole HTTP path, so with the check on it includes the guard's time.
- One dashboard panel: risk calls per second by outcome, and the p99 of `ledger.risk.duration`.

### 4.6 Letting the risk engine's containers read `ledger.entries`

tally's Kafka advertises only `localhost:9092`, which a container in another Compose project can't use: inside that container `localhost` is the container itself. tally's `compose.yaml` adds a second listener that containers can reach:

```yaml
KAFKA_LISTENERS: HOST://:9092,CONTROLLER://:9093,DOCKER://:9094
KAFKA_ADVERTISED_LISTENERS: HOST://localhost:9092,DOCKER://host.docker.internal:9094
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: HOST:PLAINTEXT,CONTROLLER:PLAINTEXT,DOCKER:PLAINTEXT
ports:
  - "127.0.0.1:9092:9092"
  - "127.0.0.1:9094:9094"
```

This relies on Docker Desktop forwarding `host.docker.internal` to the Mac. tally's Phase 10 Verify confirmed it: a throwaway `apache/kafka:4.3.1` container read an event through `host.docker.internal:9094`.

## 5. Out of scope for this tally phase

- The risk engine itself: rules, features, the decision log.
- A review or release flow for held transfers (tally still has no pending state).
- Asking the guard for entries or reversals (D8).
- Telling the guard what happened after it decided (posted, or refused with 404 or 422).
- A circuit breaker that stops calling an unreachable guard for a while (during an outage every transfer waits out the 100 ms). A good follow-up idea for tally's PROGRESS.md.
- `Retry-After` on the 503, and authentication between tally and the risk engine (both run only on this machine).

## 6. Phase text for tally's PLAN.md

Added to tally's PLAN.md under D12 (tally PR #13) and built in tally PR #14. Kept here as the plan that build followed; tally's copy is the one to change.

```markdown
## Phase 10 · Risk decision call

Before a transfer opens its posting transaction, tally asks the risk engine (project 2) whether it may go ahead. The full contract and the reasons are in the risk engine repo, docs/tally-integration.md, decisions D2 to D11. The check is off by default.

Tasks
1. `V7__entry_risk.sql`: add `decision_id uuid` (nullable) and `risk_check text not null default 'none'` with `check (risk_check in ('decided', 'fallback', 'none'))` to `journal_entries`, plus a check that `decision_id` is not null exactly when `risk_check = 'decided'`.
2. `JournalEntry` gains `decisionId` and `riskCheck`. Every place that builds a `JournalEntry` (insert, find for reversal, replay) carries them. Decide and document how a replay of a response saved before V7 shows the missing fields. Update tests that assert an exact entry body.
3. `risk/` package: `RiskProperties` (`tally.risk.enabled`, `url`, `timeout`, `on-timeout-default`, validated at startup when enabled), a `RiskClient` that posts the decision request and returns a sealed result (decided with id, decision and onTimeout; conflict; no decision with a reason), and the remembered latest `onTimeout`. The client is built from Spring Boot's auto-configured builder (for the trace header) and ends every call within `timeout` plus a small margin.
4. A non-transactional step before `PostingService.postTransfer` (outside the `@Transactional` method): replay check by key, then the guard call, then the outcome (allow or backup allow posts with the two fields; hold or block gives a 403 problem with `decisionId`; backup hold or block gives a 503 problem; the guard's 409 gives the existing key-conflict 409). Only `POST /transfers`; entries and reversals get `riskCheck` `none`.
5. Three problem types in `ApiExceptionHandler`: `/problems/transfer-held` (403), `/problems/transfer-blocked` (403), `/problems/risk-unavailable` (503).
6. Observability: timer `ledger.risk.duration` tagged by outcome with histogram buckets; `decisionId` in the log context of the posted-entry line; one WARN line per backup answer; one dashboard panel.
7. `compose.yaml`: a second Kafka listener advertised as `host.docker.internal:9094` and published on `127.0.0.1:9094`, so containers in other Compose projects can read `ledger.entries`.
8. Tests in `RiskDecisionIT`, with real PostgreSQL and Kafka, and a fake guard on the JDK's built-in `com.sun.net.httpserver.HttpServer` (no new dependency):
   1. allow: 201 with the fake's `decisionId` and `riskCheck` `decided`; the outbox payload carries both.
   2. hold: 403 `transfer-held` with `decisionId`; no entry, lines, balance change or outbox row; the key isn't saved, and a retry asks the fake again.
   3. block: 403 `transfer-blocked`, with the same checks as hold.
   4. a fake that accepts the connection and never answers, and one that answers after twice the timeout: tally answers within `timeout` plus the margin the test states, using the backup answer.
   5. connection refused, a 500, a body that isn't JSON, and a decision outside allow, hold and block: each uses the backup answer.
   6. backup default `hold`: 503 `risk-unavailable` and nothing posts. A fake that once replied `onTimeout: allow` and then goes silent: the next transfer posts with `riskCheck` `fallback` and `decisionId` null.
   7. the fake answers 409: tally answers its key-conflict 409.
   8. a key that already has a saved response: the fake is not called, and the saved 201 is replayed.
   9. the request the fake receives has exactly the fields of the contract, `requestId` equal to the key, and `requestedAt` between the test's clock readings before and after the call.
   10. while one transfer waits on a slow fake, another transfer between the same two accounts (answered allow at once) commits, which shows no lock or transaction is held during the wait.
   11. the check off (the default): the fake is never called, and entries get `riskCheck` `none`. With it on, `POST /entries` and a reversal never call the fake.
   12. the V7 check refuses `risk_check = 'decided'` without a `decision_id`, and a `decision_id` without `decided`.
9. Docs: `docs/design.md` event section lists `decisionId` and `riskCheck`; README's API section gains the 403s, the new 503 and the `tally.risk.*` settings; CLAUDE.md's status list gains 403 and the risk-unavailable 503; `docs/adr/0007-risk-call-before-the-posting-transaction.md` (context, decision, consequences, alternatives: calling inside the transaction, calling after commit, asynchronous review).

Verify: `./gradlew build` (all tests, the existing ones unchanged apart from the exact-body updates of task 2). Then `TALLY_RISK_ENABLED=true ./gradlew bootRun` with nothing on the risk URL: one transfer with `curl -w '%{time_total}'` answers 503 `risk-unavailable`, and the printed time is within the timeout plus the margin. Then, from a throwaway container, `/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server host.docker.internal:9094 --topic ledger.entries --from-beginning --max-messages 1` (image `apache/kafka:4.3.1`) prints an event that carries `decisionId` and `riskCheck`.

Done when: every test passes, the container consumer reads an event with both new fields, and tally with the check off behaves exactly as before.
```

## 7. What the risk engine does with this

For context only; built in this repo's phases.

- Serves `POST /v1/decisions` as in 3.1 and 3.2, including the 409, and the current rule set's `onTimeout` in every 200, a replayed decision included.
- Keeps every 200 body under 16 KiB, and an idle connection from tally open for at least 5 s (4.3).
- Its ledger stub mimics tally's new behaviour and event fields, so CI here can test without tally.
- After tally's phase 10, captures a fixture from a real tally event (with the new fields) and tests that the stub's events parse the same way.
- Its consumer reads `ledger.entries` from tally's broker through the container listener (4.6) when it runs against real tally, and from its own broker when it runs against the stub.
- Uses `decisionId` from the event to join decisions to entries, and `riskCheck: fallback` to count money that moved unchecked.
