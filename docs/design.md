# Design

The contracts, data, behaviour and tests behind [requirements.md](requirements.md) and [architecture.md](architecture.md). Reasons live in [decisions.md](decisions.md) (D-numbers); the contract with tally is [tally-integration.md](tally-integration.md), whose tally-side copy is tally's `docs/risk-decision-call.md` (D12).

## 1. Contracts

### 1.1 Decision API: `POST /v1/decisions`

The request and the 200 body are tally-integration.md 3.1 and 3.2, which this service must honour exactly. On top of that:

| Field | Rule (else 400) |
|---|---|
| `requestId` | string matching `[\x21-\x7E]{1,255}` (tally's Idempotency-Key pattern, D1) |
| `fromAccountId`, `toAccountId` | canonical UUIDs, different from each other |
| `amount` | JSON integer, at least 1, at most 2^63 − 1; a float or a string is refused |
| `currency` | `^[A-Z]{3}$` |
| `requestedAt` | ISO-8601 with an offset; converted to UTC and truncated to the millisecond |

Unknown fields are ignored. Responses:

| Status | When | Body |
|---|---|---|
| 200 | a decision, new or replayed | `decisionId`, `decision`, `ruleSet`, `fired`, `onTimeout`; at most 16 KiB (D13) |
| 400 | the request breaks the table above | RFC 9457 problem; no row, no attempt |
| 409 | `requestId` already decided for a different `fromAccountId`, `toAccountId`, `amount` or `currency` (D11) | RFC 9457 problem; nothing changes |
| 503 | the decision couldn't be completed within the internal deadline, or Redis or Postgres failed (D22) | RFC 9457 problem; no row |

- A replay returns the stored `decisionId`, `decision`, `ruleSet` and `fired` with the current rule set's `onTimeout` (D13).
- `onTimeout` and `decision` are always lowercase; `decisionId` is a canonical UUID v4 (D40f).
- The server keeps idle connections for 10 s (D40e; tally needs at least 5 s).
- `GET /health` answers 200 once Redis and Postgres answer; `GET /metrics` serves Prometheus text (D40t).

### 1.2 Ledger event: `ledger.entries`

The format is tally's (D1, D7, D13): key = entry id; value = the entry JSON with `id`, `kind` (`transfer`, `entry`, `reversal`), `reversesId`, `description`, `effectiveAt`, `createdAt`, `decisionId`, `riskCheck` (`decided`, `fallback`, `none`) and `lines` (each `id`, `accountId`, `amount`, `currency`). One header, `traceparent`: the W3C trace context of the relay batch that published the event (D44). The consumer:

- parses JSON; it never compares bytes, and ignores unknown fields;
- ignores headers;
- accepts timestamps with 0, 3, 6 or 9 fraction digits and a `Z`, and truncates them to the millisecond;
- reads a missing `decisionId` as null and a missing `riskCheck` as `none` (events from before tally's V7);
- treats as a bad event (stored, counted, skipped; D40i): a value that isn't JSON, a missing or wrongly typed required field, a key that differs from `id`, an unknown `kind`, no lines, a zero line amount, or lines that don't sum to zero per currency.

### 1.3 Ledger stub: `POST /transfers`

tally's transfer path (D24), on port 8001:

- Request: tally's `Idempotency-Key` header and transfer body (`fromAccountId`, `toAccountId`, `amount`, `currency`, optional `description`, optional `effectiveAt`).
- Responses, as tally gives them: 201 with the entry (the same JSON as its event); 400 validation; 403 `/problems/transfer-held` or `/problems/transfer-blocked` with `decisionId`; 409 for a key reused with a different body or a 409 from the decision API; 503 `/problems/risk-unavailable`; 503 when its own Postgres fails.
- The same key and body replay the saved 201 without asking the decision API. Only a posted transfer saves its key.
- The decision call follows tally-integration.md 3.2 to 3.4: 100 ms for the whole call, the backup answer from the latest `onTimeout` with default `hold`, a body over 16 KiB or breaking the contract treated as no decision (D40k).
- At most 100 decision calls are in flight at once, as in tally; a transfer that finds 100 in flight is no decision at once, outcome `unavailable`, and gets the backup answer (D48.5).
- Each step on the stub's own Postgres gives up after 5 s and answers 503, so a frozen database never hangs the caller; tally's bound on a silent network is its 35 s socket timeout (D48.3).
- Posting writes the entry to `stub.transfers` and its event to `stub.outbox` in one transaction; a relay loop publishes the outbox every 200 ms in batches of 100, keyed by entry id (D40k), each event with a `traceparent` header, one trace per batch, as tally's relay sends (D44). The interval is configurable, and 0 publishes at once.
- Timestamps are written like tally's: UTC, `Z`, a fraction of 0, 3 or 6 digits.
- Replay mode (2b, `STUB_REPLAY=1` only): the `X-Replay-Time` header sets `requestedAt`, `createdAt` and `effectiveAt` (D40v), and every transfer is posted whatever the decision, with `riskCheck: decided` and its `decisionId` (D40u). In replay mode a transfer without a decision is answered 503 and not posted, so a replay never logs a transfer without a decision.

### 1.4 Rule file

`risk/rules/ruleset.yaml` (D40n), loaded once at startup (D27):

```yaml
version: 1
default: allow
on_timeout: hold
rules:
  - id: velocity_1m
    action: hold
    all:
      - { feature: out_count_1m, op: ">=", value: 5 }
  - id: attempt_burst_1m
    action: block
    all:
      - { feature: attempt_count_1m, op: ">=", value: 10 }
  - id: daily_outflow_24h
    action: hold
    all:
      - { feature: out_sum_24h, op: ">=", value: 2500000 }
  - id: large_transfer
    action: hold
    all:
      - { feature: amount, op: ">=", value: 1000000 }
  - id: dest_fan_in_1h
    action: hold
    all:
      - { feature: dest_in_count_1h, op: ">=", value: 20 }
  - id: pass_through_1h
    action: hold
    all:
      - { feature: in_sum_1h, op: ">=", value: 200000 }
      - { feature: amount, op: ">=", value: 200000 }
```

- Schema: `version` an integer of at least 1; `default` is `allow`; `on_timeout` one of `allow`, `hold`, `block`; `rules` a list of at most 100 (D40x). Each rule: `id` matching `[a-z0-9_]{1,64}` and unique; `action` `hold` or `block`; `all` a non-empty list of conditions. Each condition: `feature` one of the 31 names (1.5), `op` one of `>`, `>=`, `<`, `<=`, `==`, `!=`, `value` an integer. Unknown keys are errors.
- Anything else stops the service at startup with a message naming the problem (D27).
- `ruleSet` in responses is `"v" + version`; the decision row stores the version and the SHA-256 of the file's bytes.
- Attempt features exclude the current request, so `attempt_burst_1m` blocks the 11th request within a minute.

### 1.5 Features

31 features (D19, D40b). `W` is `1m`, `1h` or `24h`.

| Name | Meaning |
|---|---|
| `out_count_W`, `out_sum_W` | entries in which the sender's net change was negative: how many, and the sum of their absolute nets |
| `in_count_W`, `in_sum_W` | entries in which the sender's net change was positive |
| `dest_in_count_W`, `dest_in_sum_W` | entries in which the receiver's net change was positive |
| `dest_out_count_W`, `dest_out_sum_W` | entries in which the receiver's net change was negative |
| `attempt_count_W`, `attempt_sum_W` | the sender's earlier decision requests and their amounts (D18) |
| `amount` | the request's amount |

All values are integers in minor units or counts, computed with Python integers (which don't overflow) and stored as JSON numbers.

### 1.6 Configuration

Every setting comes from the environment, with `.env.example` holding throwaway local values.

| Variable | Default | Source |
|---|---|---|
| `RULES_PATH` | `risk/rules/ruleset.yaml` | D40n |
| `DECISION_DEADLINE_MS` | `60` | D40d |
| `KEEP_ALIVE_SECONDS` | `10` | D40e |
| `LATENESS_MARGIN_SECONDS` | `300` | D40c |
| `CONSUMER_GROUP` | `risk-features` | D48.6 |
| `CONSUMER_START` | `earliest`; `latest` makes a group with no committed offsets start at the topic's end | D48.6 |
| `REDIS_URL`, `DATABASE_URL`, `KAFKA_BOOTSTRAP` | the Compose services | D31 |
| `STUB_RISK_URL` | `http://api:8000` | D31 |
| `STUB_RISK_TIMEOUT_MS` | `100` | D4 |
| `STUB_ON_TIMEOUT_DEFAULT` | `hold` | D10 |
| `STUB_RELAY_POLL_MS`, `STUB_RELAY_BATCH` | `200`, `100` | D40k |
| `STUB_REPLAY` | `0` | D40u |

## 2. Data model

Tables are created by numbered SQL migrations (D40w).

**`decisions`** (the decision log and the attempt log):

| Column | Type | Constraint |
|---|---|---|
| `decision_id` | uuid | primary key |
| `request_id` | text | not null, unique |
| `from_account_id`, `to_account_id` | uuid | not null, different |
| `amount` | bigint | not null, > 0 |
| `currency` | text | not null, `^[A-Z]{3}$` |
| `requested_at` | timestamptz | not null |
| `decided_at` | timestamptz | not null, the service's clock, for latency and audit only |
| `decision` | text | not null, in (`allow`, `hold`, `block`) |
| `rule_set_version` | int | not null, ≥ 1 |
| `rule_set_hash` | text | not null, 64 hex characters |
| `fired` | jsonb | not null, a JSON array of strings |
| `features` | jsonb | not null, a JSON object holding all 31 names (`features ?& array[...]`) |
| `feature_lag_ms` | bigint | not null |
| `shadow_score`, `shadow_model` | double precision, text | 2b (Phase 8), nullable |

These constraints are I5's schema half. An index on `(from_account_id, requested_at)` serves rebuilding attempts.

**`bad_events`**: `id`, `topic`, `kafka_partition`, `kafka_offset`, `message_key`, `message_value` (bytea), `error`, `received_at`; unique on `(topic, kafka_partition, kafka_offset)`, so a replayed bad message is stored once.

**`stub.transfers`**: `idempotency_key` (primary key), `request_hash`, `response_status`, `response_body` (jsonb), `entry_id`, `decision_id`, `risk_check`, `created_at`. **`stub.outbox`**: `id` (bigserial), `entry_id`, `payload` (jsonb), `created_at`, `published_at`, with a partial index on unpublished rows.

**Redis**: architecture.md section 4. Sorted-set members are `{amount}:{id}`; scores are epoch milliseconds. The sorted sets get a wall-clock expiry of 25 h after each write as memory hygiene only (D40r); correctness never depends on it, because every read filters by score. `wm` never expires (D45).

## 3. Features in detail

### 3.1 Windows

- An event counts in window `W` at decision time `t` when `t − W ≤ time < t` (D40a), with `time` = `createdAt` for ledger events and `requestedAt` for attempts, both truncated to the millisecond. Two times in one millisecond compare equal, so an event in the same millisecond as `t` doesn't count; the offline pipeline truncates the same way.
- `t` is the request's `requestedAt` (D14). The service's wall clock is never an input to a feature.

### 3.2 From an entry to members (consumer)

1. Parse and validate (1.2). A bad event goes to `bad_events`.
2. `reversal`: skip (D17). `transfer` and `entry`: sum the lines per account. A negative net adds member `{abs(net)}:{entryId}` to `f:{account}:out`; a positive net adds it to `f:{account}:in`; zero adds nothing.
3. If `createdAt` < watermark − 24 h − 5 min, the event is dropped and counted (D15).
4. Members are added with `ZADD NX`, so a duplicate is a no-op (I2). One Redis script per event adds the members, raises `wm` to `max(wm, createdAt)` and trims each touched set below `wm − 24 h − 5 min`.
5. Offsets are committed after the writes for the messages they cover (D40h). A crash in between replays those messages, which changes nothing.

Several consumer instances may share the consumer group; the script keeps the watermark a maximum, so the order in which they write doesn't matter.

### 3.3 Attempts (decision API)

After the decision row commits, and only for a new `requestId`, the API adds `{amount}:{requestId}` at score `requestedAt` to `a:{sender}` and trims that set below `requestedAt − 24 h − 5 min` (D18, D40g). If that write fails, the decision is still returned; the failure is counted, and the attempt is restored by the rebuild in 3.5.

### 3.4 Reading features (decision API)

One Redis script fetches, for the five sets of a decision (sender `out`, `in`, `a`; receiver `in`, `out`), the members with score in `[t − 24 h, t)`, plus `wm`. The shared feature module turns those members and `t` into the 31 features; the script does no arithmetic (D40y). `feature_lag_ms = t − wm`, which is negative when the stream is ahead of `t`.

### 3.5 One module, and rebuilding

- `risk/features/` holds the only feature logic: netting an entry, window membership, the 31 features from members and `t`, and the names. The consumer, the API and (2b) the offline pipeline call it. The brute-force oracle in the tests is deliberately separate, because it checks the module.
- Redis holds no state that can't be rebuilt (D40r): `make rebuild-features` clears the feature keys, replays `ledger.entries` from the offsets for time `wm − 24 h − 5 min`, and rebuilds attempt sets from `decisions`. When `wm` is gone too, as after a flush, it replays from the start of the topic; older events are then dropped as late or trimmed, so the result is the same (D48.1). This needs the topic to keep at least 24 h 5 min of messages, which Kafka's default retention exceeds.

## 4. The decision path

1. Validate (1.1). 400 on failure.
2. Start the 60 ms deadline (D40d). Redis and Postgres calls also carry their own timeouts below it, so a hung socket can't outlive the deadline.
3. Look up `request_id`. If found: the same transfer gets the stored decision with the current `onTimeout`; a different one gets 409.
4. Fetch members and compute the 31 features (3.4).
5. Evaluate the rules: every rule whose conditions all hold fires; the answer is the strictest fired action, block over hold, else `default`; `fired` lists the fired ids in file order (D21).
6. (2b) Score the shadow model on the features (section 8).
7. `INSERT ... ON CONFLICT (request_id) DO NOTHING RETURNING decision_id`. No row returned means a concurrent request won: read its row and continue as step 3.
8. Once the row has committed the deadline no longer applies: record the attempt (3.3) and answer 200.

If the deadline passes before step 7 commits, or Redis or Postgres fails, the answer is 503 and nothing is stored (D22). An insert cut off by the deadline may or may not have committed; a retry then finds the row or decides afresh, and either way one `requestId` never gets two different decisions (I1).

## 5. Rules

- Loaded and validated once at startup (1.4, D27). The loaded rule set is immutable for the life of the process.
- Every condition compares an integer feature with an integer value. No expressions, no `any`; an OR is two rules (PLAN.md section 2).
- Evaluation is pure: the same features and rule set always give the same decision and `fired`, which is what makes I8 and the backtest exact.

## 6. Invariants

| # | Invariant | How it is made true | Test |
|---|---|---|---|
| I1 | The same `requestId` always returns the same decision | unique `request_id`; insert-or-read-the-winner (section 4); replays return the stored row | `tests/invariants/test_i1_same_request.py`: Hypothesis generates request sets with duplicates and sends them concurrently to the running API; one row per `requestId` and identical `decisionId`, `decision`, `ruleSet`, `fired` per `requestId`; a changed transfer gets 409 |
| I2 | Duplicate or reordered events never change feature values | one member per entry, `ZADD NX`, results independent of order | `tests/invariants/test_i2_duplicates_order.py`: Hypothesis generates entries across a few accounts, then a delivery sequence with repeats and shuffles; features from Redis equal the oracle's over the distinct entries |
| I3 | Window values equal a recompute from the raw event log | exact windows (D16), late events applied (D15) | `tests/invariants/test_i3_recompute.py`: the same oracle at random query times within the retained range, late events included |
| I4 | Past its timeout, the caller applies `on_timeout` and never hangs | the stub's single deadline over the whole call; the API's 60 ms deadline | `tests/invariants/test_i4_caller_never_hangs.py`: the API paused, a fake that accepts and never answers, one that answers late, one that sends headers and no body, a 500, a body over 16 KiB, invalid JSON; each ends within 100 ms + 100 ms (D40m) with the backup answer, and the 503 or `fallback` that follows from it |
| I5 | Every decision row stores the rule-set version, the fired rules and a feature snapshot | the `decisions` constraints (section 2) | `tests/invariants/test_i5_decision_row.py`: inserts missing each field are refused; a row from a real decision holds the version, the hash, `fired`, all 31 features and the lag |
| I6 | Features use only events before the decision time | half-open windows (3.1) in the shared module | `tests/invariants/test_i6_point_in_time.py`: Hypothesis adds events at or after `t`; features at `t` don't change, online and offline |
| I7 | Offline features equal the features logged online | one module; the replay waits for each event (D36); record-only replay (D40u) | `tests/invariants/test_i7_parity.py` on generated PaySim-format data in CI; `make parity` on about 10k real transfers locally (D37) |
| I8 | Shadow scoring never changes a live decision | the decision is fixed before scoring, the score is never read by the decision, scoring errors are caught (D39) | `tests/invariants/test_i8_shadow.py`: the same requests with shadow off, on, and on with a model that raises, returns nonsense or is slow; decisions identical |

## 7. Failure modes

| Failure | What happens | How it shows | Test (phase) |
|---|---|---|---|
| Decision API down, paused or slow | The caller waits at most 100 ms, then applies the backup answer: 503 `risk-unavailable` under `hold`, or posts with `fallback` under `allow` | stub backup-answer counter; tally's `ledger.risk.duration` outcome tag | I4 (2), fault (5) |
| Redis down or slow | API answers 503 within 60 ms, stores nothing; caller applies the backup answer. Consumer stops committing and retries, so nothing is lost | API 503 counter; consumer error counter | fault (5) |
| Postgres down | API answers 503; the stub answers 503 as tally does for its database; the consumer keeps going and only a bad event waits | 503 counters | fault (5) |
| Kafka down | The stub's outbox grows; features go stale; decisions continue with growing lag (D23); after recovery the outbox drains and the lag falls | `feature_lag_ms`; outbox size | fault (5) |
| Consumer killed mid-batch | Restarts from the last committed offsets; replayed messages change nothing (I2) | consumer restarts | fault (5) |
| Redis data lost | Features read empty until `make rebuild-features` replays the stream and the decision log | sudden drop in feature values; watermark reset | fault (5) |
| Malformed event | Stored in `bad_events`, counted, skipped | bad-event counter | adversarial (3) |
| Event older than watermark − 24 h − 5 min | Dropped from windows, counted | late counter | I3 and adversarial (3) |
| Hot account | Its 24 h read grows with its traffic and holds Redis meanwhile (D16) | p99 against window size in the bench | bench (5) |
| Concurrent duplicate requests | One row; every caller gets that decision | | I1 (2) |
| `requestId` reused for a different transfer | 409, nothing changes | | I1 (2) |
| Attempt write fails after commit | Decision returned; the attempt is missing until a rebuild | attempt-failure counter | unit (3) |
| Invalid rule file | The service doesn't start; callers apply the backup answer | container unhealthy | unit (4) |
| Rule set changed while tally remembers an old `onTimeout` | The next 200 carries the new value (D13) | | integration (4) |
| (2b) model missing, corrupt, raising or slow | Shadow score is null; decisions unchanged | shadow error counter | I8 (8) |

## 8. 2b

- **Profile** (D32): rows and fraud per type, sender and receiver repetition, the amount distribution, rows with a zero amount. Its output is written to `ml/reports/` and confirms the sample before any feature choice.
- **Loader** (D33, D40p): TRANSFER and CASH_OUT rows; `nameOrig` and `nameDest` to UUID v5 in one fixed namespace; amount to cents, half to even; currency CAD; zero amounts skipped and counted; the rows of each hour spaced evenly through that hour in file order, at millisecond resolution.
- **Replay** (D36): through the stub in replay mode; before each request, wait until `wm` has reached the previous transfer's time. Any transfer without a decision stops the run.
- **Offline pipeline**: the loader's transfers become ledger events (one per transfer, `createdAt` = its time) and attempts (one per transfer, at the same time); in time order, the shared module gives each transfer its 31 features from strictly earlier events, plus the hour of day (D35). Online, every replayed transfer is posted and decided (D40u), so both sides see the same events.
- **Split and threshold** (D38, D34): by day of `requestedAt`; the threshold is the score above which 1% of the threshold days' transfers fall.
- **Training** (Phase 7, after D30 is confirmed): LightGBM on the training days only, with a fixed seed; the model file, its inputs and its threshold are written together with a model card.
- **Metrics on the test days**: PR-AUC; recall and fraud value caught (the sum of fraud amounts held over all fraud amounts) at the 1% hold rate; false-positive rate (non-fraud held over all non-fraud).
- **Shadow** (D39): the API loads the model at startup if one is configured; after the rules decide, it scores the 31 features and the hour, and stores the score and model name in the row. Any failure stores null.
- **Backtest CLI** (D40q): runs the rule set's evaluation function and the model over the offline features of the test days and reports both at the rules' own hold rate, plus the model at 1%.

## 9. Alternatives rejected

| Alternative | Why not |
|---|---|
| Asking inside tally's posting transaction, or after commit | Inside holds row locks and a pooled connection while waiting; after commit can't stop the money (tally ADR 0007, D2) |
| Committed-event features only | Blind to bursts inside the relay delay, and to refused attempts (D18) |
| Provisional reservations reconciled on commit | Needs a join the event couldn't give at the time, and state to expire (D18) |
| Bucketed or multi-level counters | Rounded window edges or much more code; kept as the scale-up path (D16) |
| `effectiveAt` as window time | Backdating would dodge every window (D14) |
| A fixed lateness limit | A Kafka outage longer than the limit would lose transfers for good (D15) |
| Reversals cancelling their originals | Needs tombstones for out-of-order arrival and rules for reversal chains (D17) |
| Storing a backup answer as the decision | One short outage would hold a key for ever (D22) |
| A lag-based fallback | Any Kafka delay in tally would refuse every transfer (D23) |
| First-match rules, hot reload, an expression language | Order-dependent answers; typos reaching live decisions; out of scope (D21, D27, PLAN.md) |
| Feature arithmetic in Lua | Would duplicate the offline logic (D40y) |
| Kafka transactions for exactly-once consumption | Idempotent writes with at-least-once delivery give the same result with less machinery (D40h) |
| Shadow scoring after the reply | Hides the cost of promoting the model and can lose scores (D39) |
| Parity with a tolerance | Weakens I7 into "close enough" (D36) |
| A random train/test split | Leaks the future into training (D38) |
| PaySim's balance columns as inputs | They give the label away (D35) |

## 10. Prior art

- **Rules and a score side by side.** Stripe Radar lets users write rules with block, review and allow actions that run alongside its machine-learning risk score. This project keeps the same split, with hold standing for review.
- **Point-in-time correctness and online/offline parity.** Feature stores such as Feast document point-in-time joins so that training rows only see feature values from before each label's timestamp. I6 and I7 are this project's versions of that guarantee.
- **Sliding windows in Redis sorted sets.** A common rate-limiting pattern scores each event by time and counts a score range; this project adds sums and an event-time watermark.
- **Transactional outbox and idempotent consumer.** The patterns tally uses to publish and this project uses to consume (Chris Richardson's microservices.io pattern catalogue).
- **PaySim.** E. A. Lopez-Rojas, A. Elmir and S. Axelsson, "PaySim: A financial mobile money simulator for fraud detection" (EMSS 2016): the simulator behind the dataset.

## 11. Test plan

| Layer | What it covers | Where | Runs |
|---|---|---|---|
| Unit | validation, netting, window maths, rule loading and evaluation, response shapes | `tests/unit/` | CI |
| Property | I2, I3, I6 against the brute-force oracle, with Hypothesis | `tests/invariants/` | CI |
| Integration | I1, I5, the consumer, the stub round trip, the decision API over real Redis, Postgres and Kafka (Compose) | `tests/integration/` | CI, after `make up` |
| Contract | the fixtures captured from real tally (D25) against the stub's events | `tests/integration/` | CI |
| Fault | I4 and the failure table: `docker compose pause` and `kill` of each service | `tests/adversarial/` | CI |
| Adversarial | duplicate floods, late floods, malformed events, key reuse, hot account | `tests/adversarial/` | CI |
| End to end | real tally against this service (D26) | `make e2e` | local only |
| Parity on real data | I7 on about 10k PaySim transfers (D37) | `make parity` | local only |

- Hypothesis runs with a fixed profile in CI and prints the failing example, so any failure replays.
- Timing assertions use the margin in D40m and state it.
- A flaky test is a bug in the code or the test; it is never retried away or loosened (CLAUDE.md).

## 12. Measurement plan

- **Decision latency** (D40o): k6 drives `POST /v1/decisions` at a constant arrival rate, in 3-minute steps from 50 requests a second, doubling until p99 passes 50 ms or errors pass 1%. p50 and p99 are measured at the client. The stated rates are the steps run; the reported crossing is the first step whose p99 passes 50 ms. If errors pass 1%, or k6 can't hold the arrival rate, before p99 crosses, the report says so and names the highest rate held instead of claiming a crossing.
- **Through the caller**: the same steps against the stub's `POST /transfers`, so the decision call, the posting and the relay run together.
- **Timeout rate**: the stub's backup answers over its transfers, from its `/metrics`.
- **Consumer lag**: the distribution of `feature_lag_ms` over the run's decisions, and the consumer group's offset lag at the end.
- **Hot account**: one sender for every transfer; p99 against the size of its 24 h window. The window is filled through Kafka to 1,000, 10,000, 100,000 and 1,000,000 events; at each size, 50 requests a second go straight to the decision API for 3 minutes, and the run stops after the first size whose p99 passes 50 ms (D48.2).
- **Shadow cost** (2b): the latency steps with shadow off and on.
- **Recording**: each run writes its k6 summary and a conditions file (CPU, memory, OS, Docker version and VM size, image tags, git commit and whether the tree was clean, rule set version) to `bench/results/<UTC date>/`. A script turns result files into the README's tables, so every README number comes from a script (D29).

## 13. Beyond one machine (not built)

A talk topic and a follow-up, not a phase (requirements.md section 5).

- The decision API keeps no state of its own, so more instances behind a load balancer scale it; idempotency stays correct because the unique `request_id` in Postgres arbitrates.
- The consumer scales by partitions of `ledger.entries` in one consumer group; members and the watermark are idempotent and order-free, so any instance can apply any event.
- Redis shards by account: with Redis Cluster, a hash tag on the account id (`f:{accountId}:out`) keeps an account's sets in one slot, but a decision reads two accounts, so the script becomes one read per account.
- The decision log partitions by `requested_at`; its uniqueness on `request_id` would then need the partition key, or a separate key table.
- A hot account stays the limit: its windows live on one shard. Multi-level buckets (D16) bound its read cost.
