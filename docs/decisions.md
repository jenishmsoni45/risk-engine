# Decisions

Every question asked in Phase 0 and its answer, then every later decision. Each entry gives the question, the options considered, the answer, why, and what it changes. Entries are never edited after the fact; a changed decision gets a new entry that names the one it replaces.

Status (2026-10-28): every Phase 0 question is answered; requirements.md, architecture.md, design.md and phases.md are written from these decisions.

## Log

### D1. Contract re-check against tally's `main` (2026-10-27 and 2026-10-28)

PLAN.md asks Phase 0 to re-check the ledger contract against tally's `main`. Read with `gh` from `jenishmsoni45/tally`: phases 0 to 8 merged, phase 9 (write-up, PR #12) open.

- The event is unchanged since tally phase 4: topic `ledger.entries`, 3 partitions, key = entry id, value = the entry exactly as the posting endpoint returns it (one serialization feeds the outbox row and the saved idempotent response). Null fields are sent, not left out (`reversesId: null`, `description: null`). No message headers.
- Corrections to PLAN.md's sketch:
  - The Idempotency-Key pattern is `[\x21-\x7E]{1,255}`: printable ASCII without the space.
  - Timestamps are Postgres `timestamptz` values serialized with `ISO_INSTANT`, so the fraction has 0, 3 or 6 digits (read from the code; the captured fixture will confirm it).
  - A caller's `effectiveAt` has no bound, past or future. `createdAt` is Postgres `now()`, the start of the posting transaction.
  - A reversal's `effectiveAt` is its own posting time, not the original's. A reversal can itself be reversed.
- Still true after tally phase 8 (which locks a posting's accounts in one statement): lines are inserted only after the balance locks are held, so line ids on one account follow commit order.
- New in tally since PLAN.md: a database timeout or outage answers 503 with a problem body.
- tally's outbox relay is configured to poll every 200 ms in batches of 100 rows. tally's own load test (PROGRESS.md, Phase 8) found the relay publishing slower than postings arrive, so under load Kafka fell minutes behind. Committed-event features can therefore be stale by minutes under load.
- tally's Compose stack publishes ports 5432, 9092, 3000, 4317 and 4318 on 127.0.0.1, and the app listens on 8080. tally has no Dockerfile; it runs with `./gradlew bootRun`, which starts its Compose stack. The repo is private, so this repo's CI cannot fetch it without a token.

### D2. Who calls the decision API, and what integration tests run against (PLAN Q6)

- Options: only this repo's stub calls it and real tally stays an event source; add the call to real tally as a new tally phase; stub only, permanently.
- Answer: **real tally calls it**, through a new tally phase built in the tally repo. This repo writes the spec ([tally-integration.md](tally-integration.md)); the owner takes it to tally, because this repo never changes tally.
- Why: the owner wants the real end-to-end story, with tally's docs describing the feature properly.
- Consequences: the stub in this repo still exists, because CI here cannot fetch private tally; the stub's events must carry the new fields from D7. Real tally is used for a local end-to-end check (how this repo starts tally is an open question). The fixture is captured from tally after its new phase, so it shows the new fields.

### D3. What `hold` means when tally has no pending state (PLAN Q8)

- Options: refuse with 403 and a problem type; refuse with 422 like tally's other business refusals; build tally's holds stretch (authorise, capture, void).
- Answer: **refuse with 403** and an RFC 9457 problem whose `type` tells a held transfer from a blocked one. The body carries the `decisionId` and never the fired rules.
- Why: 403 says "understood, refused by policy"; 422 says "fix your request", which a held customer can't. Fired rules stay in the decision log so an attacker can't tune against them.
- Consequences: nothing posts on hold or block. Under I1 a held Idempotency-Key stays held; there is no release flow, and a new attempt needs a new key. tally's list of status codes gains 403.

### D4. Latency budget and the caller's timeout (PLAN Q4)

- Options: guard p99 within 25 ms with a 50 ms timeout; within 50 ms with 100 ms; within 100 ms with 250 ms.
- Answer: **the caller waits at most 100 ms; the decision service's budget is p99 within 50 ms.**
- Why: room for Python and Docker on a laptop while a transfer still feels instant; tally's own posting time (its PROGRESS.md, Phase 8) is well under that at light load.
- Consequences: Phase 5 publishes p50 and p99 at stated rates and the rate where p99 crosses 50 ms. Where p99 is measured and which rates are stated are part of the measurement plan (open).

### D5. The starter rule set's backup answer

- Options: `on_timeout: hold` (fail closed); `on_timeout: allow` (fail open).
- Answer: **hold** (fail closed).
- Why: no money moves unchecked; it is the "tested fail-closed path" in PLAN.md's resume line.
- Consequences: while the guard is down or slow, every checked transfer is refused (see D6 for what the customer gets).

### D6. What the customer gets when the backup answer refuses

- Options: 503 "try again later"; 403 like a real hold.
- Answer: **503** with its own problem type (risk check unavailable). A real hold or block stays 403.
- Why: nothing was decided, and a retry with the same key asks the guard again, so the customer should be told to retry rather than told they were held.
- Consequences: three new problem types in tally: transfer held (403), transfer blocked (403), risk check unavailable (503).

### D7. Joining a decision to the entry it allowed (PLAN Q11)

- Options: caller-side mapping only; tally saves the decision id; tally adds the Idempotency-Key to the event; an outcome callback API; no join.
- Answer: **tally saves the decision on the entry**: `decisionId` (the guard's id, or null) and `riskCheck` = `decided`, `fallback` or `none`. Both fields go on the stored entry, the 201 body and the Kafka event.
- Why: tally is changing anyway, so the join costs two fields; tally also gains an audit record of which decision let each transfer through, and unchecked money (`fallback`) is easy to find.
- Consequences: the event contract gains two fields (always present). The stub must emit them. `decisionId` must be a UUID, because tally stores it in a `uuid` column.

### D8. Which tally operations ask the guard

- Options: transfers only; transfers and multi-line entries.
- Answer: **transfers only** (`POST /transfers`). Entries and reversals never ask and get `riskCheck: none`.
- Why: customer money movement is what fraud targets; entries and reversals are internal bookkeeping, and the decision request is shaped for one sender and one receiver.

### D9. tally's risk check default

- Options: off by default; on by default.
- Answer: **off by default**, switched on in tally's config.
- Why: tally behaves exactly as today without the risk engine; its existing tests, simulation and load runs stay unchanged.

### D10. Where tally gets the backup answer

- Options: from the guard's latest reply, with a config default; tally's config only.
- Answer: **every guard reply carries its rule set's `onTimeout`, and tally remembers the latest one.** Before the first reply (and after a restart) tally uses its own config value, default `hold`.
- Why: keeps the backup answer per rule set, as PLAN.md asks, without changing tally's config when the rule set changes.

### D11. The same requestId sent for a different transfer

- Options: 409; return the stored answer anyway; decide again as new.
- Answer: **the guard answers 409, and tally passes it on as its own 409** ("Idempotency-Key was already used for a different request"). A retry with the same key and the same transfer gets the stored answer, whenever it comes; `requestedAt` is not part of the match.
- Why: it matches tally's own key rule exactly, and keeps I1 (one key, one answer).
- Consequences: the guard compares `fromAccountId`, `toAccountId`, `amount` and `currency` of a repeated `requestId` with the stored ones.

### D12. One-time exception: adding the plan to tally from here (2026-10-28)

- Question: CLAUDE.md says never change the tally repo from here. May the tally spec and phase be added to tally from this session?
- Options: commit in tally locally and the owner pushes; also push and open the PR; keep the rule.
- Answer: **a one-time exception, committed locally; the owner pushes.** In the local clone `~/Desktop/tally`, branch `docs-phase-10-plan` from `main` (after tally PR #12 merged) has two commits: `docs/risk-decision-call.md` (this repo's spec, written from tally's side) and "Phase 10 · Risk decision call" in tally's PLAN.md. Nothing was pushed.
- Why: the owner asked for the plan to live in tally's own docs before building it there.
- Consequences: the rule stands for everything else. The contract now has two copies, [tally-integration.md](tally-integration.md) here and `docs/risk-decision-call.md` in tally; tally builds from its copy, and any change to the contract is made in both.

### D13. tally built the decision call; what it refined (2026-10-28)

tally PR #13 (the plan from D12) and PR #14 (its Phase 10) are merged. Checked against tally's `main`: the request, the 200 fields, the 409, the three problem types (kept as relative URIs), the backup answer, the two entry fields, V7 and the container listener match [tally-integration.md](tally-integration.md). tally's Verify read an event through `host.docker.internal:9094` from a container. The owner agreed these refinements in tally, now folded into this repo's copy:

- Replaces part of D10: every 200, a replayed decision included, carries the **current** rule set's `onTimeout`, not the one stored with the decision. I1 therefore covers `decisionId`, `decision`, `ruleSet` and `fired`.
- A 200 body over 16 KiB is no decision.
- tally closes an idle connection to the risk engine after 4 s, so **the risk engine must keep an idle connection open for at least 5 s**.
- At most 100 calls in flight per tally process; a call over that gets the backup answer at once.
- tally's parsing is strict where it matters and lenient elsewhere: canonical UUID, exact lowercase `allow`/`hold`/`block`, unknown fields ignored, no body read on a 409, no redirects followed.
- A replay of a response saved before tally's V7 shows `decisionId: null, riskCheck: none`.

Consequences for this repo: the decision API's replay path returns the stored decision with the current `onTimeout`; the server's keep-alive timeout is set explicitly to at least 5 s; the fixture can now be captured from real tally (O12), and the stub must mirror tally's Phase 10 behaviour (O11).

### D14. Which time drives the windows (PLAN Q10, O1)

- Options: `createdAt` (tally's clock); `effectiveAt` (the caller's, any date).
- Answer: **`createdAt`.** The decision time t is the request's `requestedAt`, which tally sets from the same clock. Feature logic never reads the risk engine's wall clock.
- Why: `effectiveAt` can be backdated, so a transfer could be kept out of every window. One clock on both sides means "before t" is well defined, and replays (2b) stay deterministic.
- Consequences: a window W at t holds the events with `t − W ≤ createdAt < t`. `createdAt` is the start of tally's transaction, so on one account it can run slightly out of commit order; the windows never depend on arrival order anyway (I2).

### D15. How late an event may be (PLAN Q5, O2)

- Options: no fixed limit, drop only what can't count; a fixed allowed lateness.
- Answer: **no fixed limit.** An event is applied whenever it arrives if it can still fall in a window: `createdAt` no older than the 24 h window plus a small margin behind the watermark (the latest `createdAt` applied). Older events are dropped from the windows and counted on a metric; their offsets are still committed.
- Why: tally's outbox holds events for as long as Kafka is down, and under load Kafka falls minutes behind (D1). A fixed limit would permanently lose transfers that tally only delayed.
- Consequences: the watermark is event time, not wall clock. I3 holds exactly for query times inside the retained range; design.md fixes the margin.

### D16. Exact windows or buckets (PLAN Q2, O3)

- Options: an exact sorted set per account; fixed time buckets; multi-level buckets.
- Answer: **exact**: one Redis sorted set per account and direction, scored by `createdAt`, one member per entry.
- Why: re-adding a member is a no-op and order doesn't matter, so I2, I3, I6 and I7 are plain equality with a brute-force recompute.
- Consequences: a sum reads every event in the window, so a busy account's 24 h read grows with its traffic and holds Redis meanwhile. Phase 5's hot-account test measures where that breaks; multi-level buckets are the documented way up if it does.

### D17. Which entries feed the features (PLAN Q9, O4)

- Options: transfers and multi-line entries; transfers only; reversals cancel their originals.
- Answer: **transfers and entries, netted per account per entry; reversals are ignored.** An entry moving −100 and +30 on one account is one outflow of 70; a net of zero adds nothing. A reversal doesn't remove its original from the windows.
- Why: order-independent with no extra state. Cancelling would need tombstones, because a reversal can arrive before its original (different key, different partition), and reversals can themselves be reversed.
- Consequences: sums overstate the net money moved after a refund. Each account holds one currency, so per-account sums never mix currencies.

### D18. Counting bursts that haven't committed yet (PLAN Q1, O5)

- Options: also count decision requests ("attempts"); committed events only; provisional reservations reconciled later.
- Answer: **attempt features as well.** The decision API records every request in a per-sender sorted set (member = `requestId`, time = `requestedAt`), including requests it held or blocked and transfers tally later refused. A retry with the same key counts once.
- Why: committed events trail each transfer by at least tally's relay interval and by minutes under load (D1), so a fast burst sees empty windows. Refused attempts never commit at all, yet they are the signal for probing.
- Consequences: one more Redis write on the decision path. Features at t exclude the current request. The log behind attempt features is the decision log, so I3 for attempts means a recompute from decision rows.

### D19. The feature catalog (O6)

- Options: full set (sender out and in, receiver in and out, sender attempts, amount); sender only; a middle set.
- Answer: **the full set**: count and sum over 1 min, 1 h and 24 h for the sender's outflows and inflows, the receiver's inflows and outflows, and the sender's attempts, plus the request's `amount`: 31 features.
- Why: receiver-side features catch accounts collecting from many senders, which PLAN Q12 expects to matter for PaySim.
- Consequences: each decision reads five sorted sets. Exact feature names are fixed in design.md.

### D20. The starter rule set (PLAN Q7, O7)

- Options offered: velocity_1m, attempt_burst_1m, daily_outflow_24h, large_transfer, dest_fan_in_1h; the owner asked for a recommendation and for any rule worth adding.
- Answer: **six rules**, `default: allow`, `on_timeout: hold` (D5). Thresholds are in minor units and assume CAD; they are starting guesses, not measurements, and 2b's backtest measures them.

| Rule | Action | Fires when | Catches |
|---|---|---|---|
| `velocity_1m` | hold | the sender made 5 or more transfers out in the last minute | a taken-over account drained in small amounts |
| `attempt_burst_1m` | block | the sender asked 10 or more times in the last minute (D18) | scripted probing, refused tries included |
| `daily_outflow_24h` | hold | the sender sent 2,500,000 or more in the last 24 h | a large drain spread over a day |
| `large_transfer` | hold | the amount is 1,000,000 or more | a single large movement |
| `dest_fan_in_1h` | hold | the receiver got 20 or more transfers in the last hour | an account collecting from many senders |
| `pass_through_1h` | hold | the sender received 200,000 or more in the last hour and the amount is 200,000 or more | money passing straight through (the PaySim pattern: a transfer to a mule, then a cash-out) |

- Why: each rule covers a different attack and reads a different feature family (committed counts, attempt counts, committed sums, the amount, receiver features, sender inflows).

### D21. When several rules fire (O8, in part)

- Options: the strictest action wins; the first match in file order wins.
- Answer: **the strictest wins** (block over hold over allow), and every fired rule is recorded, in file order.
- Why: the recorded reason is always complete, and moving a rule in the file never changes an answer.

### D22. When the risk engine's own parts are slow or down (rest of PLAN Q3, O9)

- Options: answer 503 within its own deadline and store nothing; store the backup answer as the decision; decide without the missing features.
- Answer: **503 within an internal deadline, no decision row.** The deadline sits well under tally's 100 ms, so tally applies the backup answer without waiting out its timeout.
- Why: a stored backup answer would make one short outage permanent for that key (I1), and deciding without features fails open silently.
- Consequences: only complete evaluations are decisions. The caller enforces the backup answer (tally, or the stub). design.md fixes the internal deadline.

### D23. When the event stream lags (O10)

- Options: keep deciding and record the lag; use the backup answer past a lag limit.
- Answer: **keep deciding, and record how stale the features were.** Every decision row stores the lag (its time t minus the watermark), and consumer lag is a published number.
- Why: a lag limit would turn any Kafka delay in tally into refusing every transfer. Attempt features (D18) stay fresh whatever the lag.

### D24. The ledger stub's scope (O11)

- Options: the transfer path with tally's publish delay; the same with instant publish; a fuller copy with balances, entries and reversals.
- Answer: **the transfer path, with tally's delay.** `POST /transfers` with tally's Idempotency-Key rules (replay, 409); it asks the decision API with tally's timeout, backup answer, 403s and 503 (D3 to D11, D13); on allow it publishes to `ledger.entries` in tally's exact format, `decisionId` and `riskCheck` included, through a relay-like loop that defaults to tally's poll interval and batch size (tests can set the interval to 0). No accounts or balances. Tests publish `entry` and `reversal` events directly.
- Why: CI can't run private tally; copying the delay keeps freshness and lag numbers honest.

### D25. Capturing real tally events as fixtures (O12)

- Options: captured in Phase 2 from real tally; captured by the owner.
- Answer: **captured in Phase 2**, once the decision API answers: real tally from the owner's clone (`~/Desktop/tally`) runs with the risk check on against it, and four events are recorded as fixtures: an allowed transfer (`decided`), a backup-allowed transfer (`fallback`), an `entry` and a `reversal` (`none`). The owner approves each command, because tally's folder is outside this repo.
- Consequences: a contract test parses the fixtures and the stub's events the same way.

### D26. The end-to-end check against real tally (O13)

- Options: a local-only `make e2e`; a manual checklist.
- Answer: **`make e2e`, local only, never in CI.** It uses the owner's clone (`TALLY_DIR`, default `~/Desktop/tally`), starts tally with the risk check on and pointed at this repo's decision API, has the consumer read tally's broker through `host.docker.internal:9094`, posts transfers, and checks that each decision joins its entry by `decisionId`.
- Why: a repeatable proof of the real integration that CI can't run.

### D27. How a changed rule set goes live (rest of O8)

- Options: read at startup only; hot reload.
- Answer: **startup only.** Each decision stores the rule set's version and a hash of the file. An invalid file, or a rule naming a feature that doesn't exist, stops the service from starting.
- Why: a typo or a half-saved file never reaches a live decision.

### D28. `uv` is missing on this Mac (O22)

- Options: the owner installs it; run Python only inside Docker.
- Answer: **the owner installs it before Phase 1** (Homebrew is present: `brew install uv`; the official installer is the alternative). uv then fetches Python 3.12 for the project into its own cache. Phase 1 starts by checking `uv --version` and stops if it fails.

### D29. Numbers in README.md and docs/ (O20)

- Options: the rule covers measurements only; no numbers at all.
- Answer: **measurements only.** A measured result (latency, rates, lag, timeout rate, model metrics) appears in README.md or docs/ only if a script in this repo produced it. Decided targets, thresholds and configuration values are allowed when they trace to an entry in this file.

### D30. Training and running LightGBM under CLAUDE.md's AI-model rule (O21)

- Options: allowed because the model is trained here; decide later.
- Answer: **deferred to Phase 7.** Phase 7 starts by asking the owner to confirm that a LightGBM model trained in this repo from PaySim is allowed; until then nothing trains or loads a model.
- Consequences: Phase 7's acceptance criteria open with that confirmation, recorded here.

### D31. The Compose stack (O23)

- Options: this repo's own stack on ports that don't clash with tally's; always share tally's Kafka.
- Answer: **its own stack**: Redis, Postgres and Kafka, with Kafka and Postgres on the same images as tally (`apache/kafka:4.3.1`, `postgres:18.6`), plus the decision API and the ledger stub. Host ports avoid tally's (8080, 5432, 9092, 9094, 3000, 4317, 4318): decision API 8000 (tally's default risk URL), stub 8001, Postgres 5433, Redis 6379, Kafka 19092. Every port is bound to 127.0.0.1. The decision API has no authentication, because it is reachable only from this machine. Under `make e2e` the consumer reads tally's broker (`host.docker.internal:9094`) instead of its own.
- Consequences: both stacks can run at once. Image tags not listed here are looked up and pinned in Phase 1.

### D32. Which PaySim sample (PLAN Q12, O14)

- Options: TRANSFER and CASH_OUT rows over all days; every type over all days; the first week only.
- Answer: **TRANSFER and CASH_OUT rows, all days.** Phase 6 opens with a profiling script that counts rows and fraud per type and measures how often senders and receivers repeat; its output confirms the sample and which feature sides carry the signal before features are chosen.
- Why: these are the types fraud is reported to occur in; the full month keeps every fraud case for training and testing.

### D33. Times within a PaySim hour (O15)

- Options: spread an hour's rows evenly in file order; all at the start of the hour; seeded random times.
- Answer: **evenly spaced within the hour, in file order.** The replay sets `requestedAt` and the stub's `createdAt` from those times.
- Why: deterministic, and it adds no order the file doesn't already have.

### D34. The review capacity that sets the threshold (PLAN Q13, O16)

- Options: 0.5%, 1% or 2% of transfers held.
- Answer: **1% of transfers held.** The model's threshold is the score that holds 1% of transfers, and the rules are compared with it at the same hold rate.

### D35. Which inputs the model may use (PLAN Q14, O17)

- Options: only what the guard sees; also the transaction type; also the balance columns.
- Answer: **only what the guard sees**: the 31 features of D19 plus the hour of day of `requestedAt`. No balance columns, no `isFlaggedFraud`, no account names, no transaction type.
- Why: the balance columns make PaySim artificially easy, and anything the live guard can't see would make offline results meaningless in shadow.

### D36. How the replay reaches exact parity for I7 (O18)

- Options: wait for the consumer to catch up before each decision, on a sample; replay the whole month that way; compare with a tolerance.
- Answer: **wait-for-catch-up on a sample.** Before asking about the next transfer, the replay waits until the consumer has applied the previous transfer's event. It runs on a contiguous sample of about 10k transfers (PLAN.md's size). Training features for the whole dataset come from the shared feature module offline, with no waiting.
- Why: exact equality, not "close enough", at a runtime that fits a laptop.
- Consequences: I7 compares the features logged online with an offline recompute over the same replayed event log.

### D37. What CI runs for I6 to I8 (O19)

- Options: generated PaySim-format data; run I6 to I8 only locally.
- Answer: **a seeded generator of PaySim-format rows** with planted fraud patterns; CI runs I6 to I8 on it. The parity check on the real 10k sample runs locally through a make target.
- Why: PaySim can't be committed (CLAUDE.md), and PLAN.md's definition of done wants I6 to I8 in CI.

### D38. The time split (PLAN phase 7)

- Options: by days; by share of rows in time order.
- Answer: **by days**: the last 7 days are the test set, the 2 days before them set the threshold (D34), and every earlier day trains. No row from a later day is used to train or to set the threshold.

### D39. When the shadow model scores (PLAN phase 8)

- Options: in the same request, right after the rules decide; after replying, in the background.
- Answer: **in the request, after the rules have fixed the decision and before replying**, stored in the same decision row. A scoring error stores no score and never touches the decision.
- Why: it measures what promoting the model would really cost, and one row keeps decision and score together. I8 is tested by comparing decisions with shadow on and off.

### D40. Design choices made while writing the Phase 0 docs

Not asked as questions: choices made inside the answers above while writing requirements.md, architecture.md, design.md and phases.md. Listed here so every value in those docs traces to this file (D29), and for the owner to accept or change in the Phase 0 PR review.

| # | Choice | Value | Why |
|---|---|---|---|
| a | Window membership | an event counts in window W at t when `t − W ≤ time < t` (`createdAt` for ledger events, `requestedAt` for attempts) | half-open, so an event at exactly t is not "before t" (I6) |
| b | Feature names | sender `out_*` and `in_*`, receiver `dest_in_*` and `dest_out_*`, sender `attempt_*`; each `_count` and `_sum` over `_1m`, `_1h`, `_24h`; plus `amount` | follows PLAN.md's `out_count_1m` |
| c | Lateness margin | 5 min: an event with `createdAt` older than watermark − 24 h − 5 min is dropped and counted | a decision whose t is slightly behind the watermark still sees a full 24 h |
| d | Internal deadline | 60 ms from receiving the request; past it the decision API answers 503 | under tally's 100 ms with room for the network; above the 50 ms p99 budget |
| e | Server keep-alive | idle connections kept 10 s | D13 requires at least 5 s |
| f | Identifiers | `decisionId` is a UUID v4; `ruleSet` is `"v<version>"` | tally stores a uuid (D7) |
| g | When an attempt is recorded | after the decision row commits, for a new `requestId` only | attempt features then equal a recompute from the decision log |
| h | Offset commits | after the Redis writes for the consumed messages | at-least-once delivery with idempotent writes |
| i | Bad events | a message that fails parsing or validation is stored in `bad_events`, counted, and its offset committed | one malformed message never stalls a partition |
| j | Processes | decision API, feature consumer and ledger stub run as separate Compose services built from one image | a consumer crash never stops decisions |
| k | Stub settings | relay poll 200 ms and batch 100, timeout 100 ms, backup default `hold`: tally's values | D24 |
| l | Stub storage | the stub keeps its keys, saved responses and outbox in its own Postgres schema, `stub` | a restart doesn't lose keys, and the relay works like tally's outbox |
| m | Timing margin in tests | a bounded call must end within its timeout plus 100 ms | room for scheduling on a loaded laptop or CI runner without hiding a hang |
| n | Rule file | `risk/rules/ruleset.yaml`, `version` starting at 1 | one active rule set (D27) |
| o | Bench procedure | k6 constant arrival rate, steps of 3 min starting at 50 requests a second and doubling until p99 passes 50 ms or errors pass 1%; p99 measured at the client | finds the rate where p99 crosses the budget (PLAN.md section 4) |
| p | PaySim mapping | account names to UUID v5 in one fixed namespace; amount to cents, rounded half to even; currency CAD; rows with a zero amount skipped and counted | deterministic replay that the ledger contract accepts |
| q | Backtest comparison | the rules at their own hold rate against the model at that same rate, plus the model at 1% (D34) | "better, not just stricter" (PLAN.md section 9) |
| r | Redis persistence | not relied on: windows are rebuilt by replaying `ledger.entries` from the time watermark − 24 h − 5 min, and attempts from the decision log. Keys expire 25 h after their last write, as memory hygiene only | Redis loss is a failure mode with a procedure, not data loss; the expiry is longer than any window plus the margin, and reads filter by score anyway |
| s | Libraries | runtime: fastapi, uvicorn, pydantic, redis, psycopg, confluent-kafka, httpx, pyyaml, prometheus-client; dev: pytest, hypothesis, ruff, mypy, pip-audit; 2b: lightgbm, pandas | widely used; each pinned and justified in the PR that adds it |
| t | Metrics | the decision API, the consumer and the stub each serve `GET /metrics` in Prometheus text format | the bench and fault tests read counters (decisions by outcome, 503s, backup answers, late drops, bad events, watermark) from one place; no metrics stack is run |
| u | Replay is record-only | in replay mode the stub posts every replayed transfer whatever the decision; decisions are still made and logged with their features | PaySim is history: its transfers happened and its labels refer to them, so holding one would rewrite the history the model learns from. Online and offline features then both cover every replayed transfer |
| v | Replay clock | in replay mode the stub takes the transfer's time from the replay and uses it as both `requestedAt` and `createdAt` | features follow PaySim's time (D33), not the wall clock; an event at exactly t is outside its own decision's windows (row a) |
| w | Schema migrations | numbered SQL files applied in order by a one-shot `migrate` Compose service before the others start, recorded in `schema_migrations`; a merged migration is never edited | one path for the decision log, `bad_events` and the stub's schema |
| x | Rule set limits | rule ids match `[a-z0-9_]{1,64}`, and a rule set has at most 100 rules | keeps every 200 body far under tally's 16 KiB cap (D13), whatever fires |
| y | Where feature arithmetic runs | the Redis script only fetches the members in range; counts and sums are computed in Python by the shared feature module | the offline pipeline calls the same function, so no feature logic is duplicated in Lua (CLAUDE.md) |
| z | Consumer metrics port | the consumer's `GET /metrics` is published on 127.0.0.1:8002 | tests and the bench run on the host and read its counters; the port is not one of tally's |

### D41. Keeping this Mac safe: installs, downloads and clean-up (2026-11-01)

- Question: before Phase 1, the owner asked that nothing installed or downloaded for this project be unsafe, that the work run unattended without doing anything risky, and that one command remove everything the project put on the Mac, without changing any earlier decision.
- Options: rely on exact pins and `make audit` alone (D40s, requirements.md); add install-time safeguards, checked by tests, and clean-up commands.
- Answer: **the safeguards and the clean-up commands** (owner, 2026-11-01). This replaces one sentence of D28 ("uv then fetches Python 3.12 for the project into its own cache"); the rest of D28 stands.
  1. **Release cutoff.** `[tool.uv] exclude-newer` is a fixed UTC time at least 7 days before the day it is set, so uv ignores every release newer than that. When `make audit` reports a vulnerability whose fix is newer than the cutoff, only that package's cutoff moves, with `exclude-newer-package`, in a PR that names the advisory; each such override gets its own entry here.
  2. **Wheels only.** `no-build = true`, so no package's build code runs at install. The project itself is not built as a package (`package = false`; `risk/` is imported from the repo root), so no build backend is downloaded either. A package with no wheel for this Mac (macOS arm64) or for the image's Linux makes the install fail, and the owner is asked.
  3. **No Python downloads.** `python-downloads = "never"`: uv uses the Python 3.12 already on the Mac, and the image's own Python in Docker.
  4. **Locked installs only.** The Makefile, the Dockerfile and CI install with `uv sync --locked`, which refuses a lock that doesn't match `pyproject.toml` and checks every downloaded file against the lock's hash.
  5. **Approved packages only.** Direct dependencies are exactly the D40s libraries the current phase uses. psycopg is installed as `psycopg[binary]`, which carries its own libpq, so nothing is needed on the Mac. Checked on PyPI on 2026-11-01: every D40s library has a wheel for macOS arm64 and Linux arm64 for Python 3.12. Each PR that changes `uv.lock` lists every package it adds or changes, direct and indirect.
  6. **Clean-up.** `make clean` stops the stack and removes this project's containers, volumes and network and the image built from this repo, then `.venv` and the tool caches (`.pytest_cache`, `.mypy_cache`, `.ruff_cache`, `.hypothesis`, `__pycache__`). It never touches `data/`, `.env` or any tracked file. `make purge` runs `make clean`, then removes the images this repo pins and empties uv's cache. Docker refuses to remove an image that any container still uses, so tally's containers keep theirs, and tally pulls its pinned tags again if one was removed. `make purge` ends by printing the one step left to the owner: `brew uninstall uv`.
- Why: pins and audits catch known problems after the fact. The cutoff, wheels-only and locked installs stop the usual supply-chain attacks (a fresh malicious release, code run at install time, a silently changed file) before anything runs, and tests keep them on, so they hold without anyone watching.
- Consequences: Phase 1 adds the settings, `make clean`, `make purge`, `tests/unit/test_dependency_safety.py` and three acceptance criteria (phases.md). A security fix newer than the cutoff waits for an override approved in a PR. LightGBM's macOS wheel needs OpenMP (`brew install libomp`, per its PyPI page), which can't be installed on this Mac: O24.

### D42. Gaps found at the start of Phase 1 (2026-11-01)

Four questions the docs didn't settle, asked before any Phase 1 code, plus the image tags D31 left to Phase 1.

1. **Who creates `ledger.entries`.**
   - Options: a one-shot `topics` Compose service; the stub creates it at startup, as tally does with its `NewTopic` bean; `migrate` creates it.
   - Answer: **a one-shot `topics` service** (owner). It runs on `apache/kafka:4.3.1` (no new image) once Kafka is healthy, creates the topic with `--if-not-exists`, 3 partitions and replication factor 1, and fails if the topic exists with another partition count. The consumer and the stub start only after it completes. The broker's automatic topic creation is off, so no topic appears by accident with the wrong partition count.
   - Why: the topic exists before anything reads or writes it, so the consumer never subscribes to a missing topic, and `migrate` keeps its one job (D40w).
   - Consequences: architecture.md gains the service in its component diagram and deployment table; phases.md's Phase 1 scope and criterion 2 name it.
2. **How the Docker image gets uv.** Astral's `astral/uv` image is listed on Docker Hub as a community image, neither official nor from a verified publisher, so CLAUDE.md's image rule excludes it.
   - Options: install uv from PyPI inside the image, hash-pinned; use `astral/uv` as an exception; no uv in the image (pip with a hashed export, which breaks D41.4).
   - Answer: **from PyPI, hash-pinned** (owner). A build stage installs uv 0.13.6, the version on the Mac, from `docker/uv-requirements.txt` with `pip install --require-hashes --only-binary :all: --no-deps`, then runs `uv sync --locked --no-dev`. The final stage copies only the virtual environment, without uv.
   - Note: uv 0.13.6 was uploaded to PyPI on 2026-10-27, after D41's cutoff. The cutoff governs `uv.lock`; keeping the image's uv equal to the Mac's keeps one lock format across both.
3. **How CI gets uv.**
   - Options: Astral's `setup-uv` action pinned to a commit; the same hash-pinned PyPI install on the runner.
   - Answer: **`astral-sh/setup-uv` pinned to a commit** (owner): v10.3.2 at `07a047e608db84b3d8c06cd9a7c7dcc9c355c18e`, installing uv 0.13.6 checked against the SHA-256 of its Linux release archive, with the action's cache off. `actions/checkout` is pinned the same way (v7.0.1 at `3d3c42e5aac5ba805825da76410c181273ba90b1`). Both tags are more than 7 days old, in the spirit of D41.1.
4. **Docker's build cache.** Building the image leaves layers in Docker's build cache, which `make clean` and `make purge` don't remove: Docker can't clear one project's build cache alone, and the local safety guard blocks `docker builder prune`.
   - Options: `make purge` prints it as a step for the owner; `make purge` clears all build cache itself; leave it and document it.
   - Answer: **print it as a step for the owner** (owner). `make purge` ends by printing two steps left to the owner: `docker builder prune` (which clears every project's build cache) and `brew uninstall uv`. This replaces "the one step left to the owner" in D41.6; the rest of D41 stands.
5. **Image tags** (D31 left them to Phase 1): `redis:8.10.3` and the build base `python:3.12.16-slim-trixie`, the newest patch releases on Docker Hub on 2026-11-01. architecture.md's Redis row now names the tag.

### D43. Gaps found at the start of Phase 2 (2026-11-02)

Three questions the docs didn't settle, asked before any Phase 2 code.

1. **The features snapshot before Phase 3.** Phase 2 decides without window features, yet `features` is not null.
   - Options: only `amount`; all 31 names with the window features at 0; an empty object.
   - Answer: **only `amount`** (owner). Phase 2's rows store `{"amount": <amount>}`, the one feature that exists without the consumer. Phase 3 adds the 31-name check so that it applies to new rows and leaves Phase 2's rows as they were (`not valid`).
   - Why: zeros would claim no activity where nothing was counted.
2. **The lag when there is no watermark.** `feature_lag_ms` is `t − wm` and not null, but `wm` exists only once the consumer has applied an event: not on a fresh stack, not after Redis is lost, and not at all in Phase 2.
   - Options: read a missing watermark as 0, the epoch; allow a null lag in that case only (a change to design.md that loosens I5).
   - Answer: **a missing watermark reads as 0** (owner). The lag is then `t` in epoch milliseconds, which marks "no event applied yet", and the column stays not null. Anything that reports on lag leaves such rows out.
   - Consequences: Phase 2's decision path reads `wm` from Redis; that read is the Redis call that Phase 2's criterion 6 pauses.
3. **A Postgres connection pool.** Every decision reads and writes Postgres within the 60 ms deadline, and a new connection per request repeats the login handshake each time.
   - Options: psycopg's own pool, `psycopg-pool`, from psycopg's maintainers and not on D40s's list; a small pool written here; a new connection per request.
   - Answer: **`psycopg-pool`** (owner), installed through psycopg's `pool` extra as `psycopg[binary,pool]`. This adds it to D40s's runtime list; the rest of D40s and D41 stands. It is pure Python, so one wheel serves the Mac and the image.

### D44. tally's events carry a `traceparent` header (2026-11-02)

- Found while capturing the Phase 2 fixtures (D25) from tally's `main` (PR #14's tree): every event on `ledger.entries` carries one Kafka header, `traceparent`, the W3C trace context of the relay batch that published it (tally's KafkaTemplate observation). Events published in one relay batch share a trace id and differ in span id; the flags are `03`. This replaces "No message headers" in D1; the rest of D1 stands.
- Question: design.md 1.2 said "No headers". What does the stub send?
- Options: copy the header, one trace per batch, as tally's relay does; send none and document that the consumer ignores headers.
- Answer: **copy it** (owner). The stub's relay adds a `traceparent` to each event, with a new random trace id per batch and a new random span id per event. design.md 1.2 now names the header and says the consumer ignores headers; design.md 1.3 says the stub's relay sends it.
- Why: the stub's events then match tally's in every part, so CI exercises the consumer with headers present, as real traffic has them.
- Consequences: the contract test checks that tally's fixtures and the stub's events each carry exactly this one header, in the W3C format.

### D45. Gaps found at the start of Phase 3 (2026-11-02)

Three questions, asked before any Phase 3 code: one the docs didn't settle, and two follow-ups left by the Phase 2 PR.

1. **Whether the watermark expires.** D40r gives Redis keys a wall-clock expiry of 25 h after each write, as memory hygiene, without saying whether that includes `wm`. A missing `wm` reads as 0 (D43), so an expired one would make every lag read "no event applied yet" after a quiet day.
   - Options: `wm` never expires; `wm` expires like every other key.
   - Answer: **`wm` never expires** (owner). Only the per-account sets (`f:*` and `a:*`) get the 25 h expiry. design.md section 2 now says so; the rest of D40r stands.
   - Why: it is one small key, so it needs no hygiene, and the lag stays true after a quiet day.
2. **Two rule files both called version 1.** Phase 2's empty rule set is version 1, and Phase 4's starter set is version 1 too (phases.md, Phase 4 criterion 6).
   - Options: keep both as version 1; ship the starter set as version 2.
   - Answer: **keep both as version 1** (owner). No doc changes.
   - Why: the empty set was a build stage that never made a real decision, the stored file hash tells the two apart, and the local database has been wiped since.
3. **Which Phase 2 follow-ups to fix now.**
   - Options: the Starlette deprecation warning from Phase 1's health test; the stub's cap of 100 decision calls in flight; a bound on the stub's wait for a paused Postgres; turning off uvicorn's access log.
   - Answer: **only the test warning** (owner): the health test drops Starlette's TestClient for httpx alone, with the same checks and no new package. The other three stay with Phase 5, whose bench and fault tests measure what they need.

### D46. Gaps found at the start of Phase 4 (2026-11-02)

Two questions the docs didn't settle, asked before any Phase 4 code.

1. **A key given twice in one mapping.** YAML requires the keys of a mapping to be unique, but PyYAML's loader keeps the last value of a repeated key without a word, so `value: 5` followed by `value: 50` in one condition would load as 50.
   - Options: refuse the file; keep the last value, as the library does.
   - Answer: **refuse the file** (owner). A repeated key at any level stops the service at startup with a message naming the key and its line, like any other invalid file (design.md 1.4, D27). No doc changes: design.md 1.4 already says that anything else stops the service.
   - Why: a copy-paste slip must not silently change a threshold.
2. **A changed file that keeps its version.** If the rules change and `version` doesn't, two rule sets answer as the same `ruleSet`, told apart only by the stored hash.
   - Options: leave it, as a follow-up; refuse to start when the decision log already holds the version with a different hash.
   - Answer: **a follow-up** (owner). Phase 4 adds no check; the PR lists it under Follow-ups.
   - Why: D45.2 already accepted two files with version 1, and a check would need Postgres at startup.

### D47. Kafka's log restarts while `bad_events` keeps its rows (2026-11-02)

- Found during Phase 4's `make test`: six cases of `tests/adversarial/test_bad_events.py` (Phase 3) failed locally while CI, which starts empty, passes. Kafka keeps its log inside its container, so `make down` and `make up` start `ledger.entries` again at offset 0, while Postgres keeps its volume. `bad_events` is unique on `(topic, kafka_partition, kafka_offset)` (design.md section 2), so a new bad event at an offset an earlier log already used is taken as stored and skipped. The same reset also empties the log that `make rebuild-features` (Phase 5, design.md 3.5) replays.
- Question: how to get Phase 4's run green, and when to fix the gap.
- Options: wipe the local stack's data and rerun; fix the gap in this PR (a named volume for Kafka, so its log lasts as long as Postgres's); leave it as a follow-up.
- Answer: **wipe the local data and rerun; the gap is a follow-up** (owner). The Phase 4 PR lists it under Follow-ups, next to Phase 5's rebuild and fault tests. No code or doc changes in Phase 4.
- Why: the failure comes from test data left by earlier runs, not from the rules, and keeps Phase 4 to rules only.

### D48. Gaps found at the start of Phase 5 (2026-11-02)

Seven questions the docs didn't settle, or that earlier phases left to this one, asked before any Phase 5 code.

1. **Where the rebuild starts once the watermark is gone.** design.md 3.5 replays `ledger.entries` from the offsets for `wm − 24 h − 5 min`, but a Redis flush loses `wm` with everything else.
   - Options: from the topic's start when `wm` is missing, else from the offsets for `wm − 24 h − 5 min`; always from the topic's start; keep a copy of `wm` in Postgres.
   - Answer: **from the topic's start when `wm` is missing** (owner). An event older than the retained range is dropped as late, or trimmed once newer events raise the watermark, so the result is the same as starting later, only slower. design.md 3.5 now says so.
2. **The hot-account bench.** design.md 12 asks for p99 against the size of one sender's 24 h window without saying how the window grows.
   - Options: preload the window to fixed sizes and measure each at a fixed rate; one long run at a fixed rate, grouping latencies by window size afterwards; the doubling rate steps with one sender.
   - Answer: **preloaded sizes at a fixed rate** (owner). The hot sender's 24 h window is filled through Kafka to 1,000, then 10,000, 100,000 and 1,000,000 events; at each size the bench sends 50 requests a second straight to the decision API for 3 minutes, and stops after the first size whose p99 passes 50 ms. design.md 12 now says so.
   - Why: one cause at a time, so the table shows which size breaks p99.
3. **How long the stub waits for a frozen Postgres** (left to this phase by D45.3).
   - Options: 5 s, then 503; tally's 35 s socket timeout; test a stopped Postgres instead of a paused one.
   - Answer: **5 s, then 503** (owner). Each database step of the stub gives up after 5 s, the statement timeout it already copies from tally's transaction timeout, and the stub answers tally's 503 for its database. tally waits up to its 35 s socket timeout on a silent network, so the stub answers sooner: a known difference. design.md 1.3 now says so.
4. **D47's follow-up, and the postmortem's bug.**
   - Options: fix it now and write the postmortem on it; fix it now and write the postmortem on D44; leave it as a follow-up.
   - Answer: **fix it now, and `docs/postmortem.md` tells it** (owner). Kafka keeps its log in a named volume, so the log lasts as long as Postgres's; `make clean` removes both (D41.6). This closes D47's follow-up.
5. **The stub's other D45.3 follow-ups.**
   - Options (any of): cap decision calls in flight at 100, as tally does; turn off uvicorn's access log.
   - Answer: **both** (owner). The stub has at most 100 decision calls in flight; a call over that is no decision at once, with tally's outcome tag `unavailable`, and gets the backup answer (tally's `docs/risk-decision-call.md`). The decision API and the stub write no access log line per request; their counters count requests. design.md 1.3 now names the cap.
6. **Where `make e2e`'s fresh stack lives.**
   - Options: its own Compose project, removed at the end; wiping the normal stack's volumes.
   - Answer: **its own Compose project**, `risk-engine-e2e` (owner), with its own volumes, removed at the end, so the normal stack's data is untouched. Its ports are the normal stack's, so `make e2e` first checks that the normal stack is stopped and says so if not. The consumer gains two settings, `CONSUMER_GROUP` and `CONSUMER_START`, for the new group that starts at the end of tally's topic. design.md 1.6 and architecture.md section 5 now say so.
7. **Who runs `make bench`.** Its steps take most of an hour with the laptop busy, and its results must come from a clean, committed tree.
   - Options: run during the phase once the code is committed; the owner runs it.
   - Answer: **run during the phase** (owner), once the code is committed; the results, and the README tables made from them, are committed in this phase's PR.

### D49. The rest of the bench procedure (2026-11-02)

design.md section 12, D40o and D48.2 fix the bench's steps, first rate, budget, error limit and hot-account sizes. The choices below fill in the rest. They were made while writing `make bench` (`bench/run.py`), and the owner accepted them before the bench was run. No doc changes.

| # | Choice | Value | Why |
|---|---|---|---|
| a | Holding the rate | a step also stops when k6 drops more than 1% of the requests it planned (rate × step length) | design.md 12 has the report name the highest rate held when k6 can't hold the arrival rate; this makes "can't hold" a number |
| b | Highest rate | 12,800 requests a second, eight doublings from 50 | a backstop, so a run ends even if no stop rule fires |
| c | Accounts | senders and receivers drawn at random from 10,000 accounts | load spreads over many keys, so only the hot-account scenario concentrates on one sender |
| d | Hot preload | the hot sender's transfers are published to `ledger.entries` in tally's event format, with `createdAt` spread over the 23 hours before the preload, so every one is inside the 24 h window | the window grows through the same path as real events (D48.2) |
| e | Preload wait | up to 30 min for the consumer to apply each preload; past that the bench fails | a stuck consumer ends the run with a message instead of hanging it |
| f | Fresh stack per scenario | each scenario runs on its own Compose project, `risk-engine-bench`, started empty and removed with its volumes at the end; the normal stack must be stopped first, since both use the same ports | every scenario starts from the same state and the normal stack's data is left alone, as D48.6 does for `make e2e` |
| g | Quick tries | `BENCH_STEP_SECONDS` shortens every step; the conditions file records the step length, and `bench/report.py` refuses results whose steps aren't 3 min or whose tree wasn't clean | a quick check of the runner can never reach README.md |

### D50. The hot account receives instead of sending (2026-11-03)

- Found during Phase 5's first full `make bench`: the hot sender's window didn't stay at the preloaded size during a step. Every decision records an attempt on its sender (design.md 3.3), and the attempts are one of the five sets the decision reads. At 50 requests a second for 3 minutes, the bench's own attempts grew the window to several times the first size of 1,000. p99 passed 50 ms during that first step, so the run stopped there, and the report credited the result to the 1,000 preloaded events. D48.2 asked for one cause at a time, and this run measured two.
- Question: how to measure p99 against one account's window size.
- Options: a hot receiver, with transfers from senders drawn at random; a hot sender at 5 requests a second, so its own attempts add at most 900 per step; keep the results and say what they show.
- Answer: **a hot receiver** (owner). Every request goes to one account from one of the bench's 10,000 accounts, drawn at random. That account's 24 h incoming window is preloaded through Kafka to 1,000, 10,000, 100,000 and 1,000,000 events. The sizes, the rate, the step length and the stop rule are D48.2's. design.md 12 now says so. This replaces "the hot sender" in D48.2 and in D49 rows c and d; the rest of both stands.
- Why: the decision reads the receiver's incoming set the same way it reads the sender's outgoing set, but the attempts land on the random senders. The window then stays at its preloaded size through each step, at the planned rate.
- Consequences: only the hot scenario is run again. README.md's conditions list each scenario's commit, so they show that the hot results come from a later commit than the direct and stub results.

### D51. Gaps found at the start of Phase 6 (2026-11-03)

Four questions, asked before any Phase 6 code: one found by timing the consumer, one by CI on `main`, the instant Phase 6 must choose, and O24.

1. **The consumer waits to fill a batch.** The consumer asked Kafka for up to 500 messages with a 0.5 s timeout, and librdkafka waits out the timeout unless the batch fills. Timed on this Mac at the start of the phase, an event published on its own was applied only once that timeout had run. In light traffic every event therefore waited up to 0.5 s before it counted. The replay waits for each event before the next request (D36), so a 10k sample would wait up to 0.5 s per transfer. A live decision also missed any transfer committed within the last half second.
   - Options: block for the first message, then take only what has already been fetched; a setting that only the parity stack lowers; keep it.
   - Answer: **block for the first message, then drain** (owner). Each poll waits up to 0.5 s for one message, then takes up to 499 more that have already been fetched, without waiting. A busy stream still arrives in batches of up to 500. No doc changes: design.md doesn't say how the consumer polls.
   - Consequences: README's consumer-lag numbers were measured before this change. Their conditions name the commit, and Phase 8's bench measures the new loop.
2. **The UTC instant of PaySim's step 1** (phases.md, Phase 6).
   - Options: 2026-01-01T00:00:00Z with step s covering `[base + (s − 1) h, base + s h)`; the same date with the hour of day equal to `s mod 24`; choose after the profile.
   - Answer: **step 1 starts at 2026-01-01T00:00:00Z** (owner). The hour of day (D35) is `(s − 1) mod 24`, and day d (D38) holds steps `24d + 1` to `24d + 24`, from UTC midnight. The profile prints rows per hour of day under this mapping, so the owner can check where the nightly lull falls when confirming the sample.
   - Why: it is in the past, so a replay never pushes the watermark ahead of the wall clock, and the stack's later events are never late behind it.
3. **A timing-dependent test on `main`.** After the Phase 5 merge, CI on `main` failed in `tests/adversarial/test_floods.py`: in the burst of 200 concurrent requests with one `requestId`, every request got 503 within the 60 ms deadline on the slower runner, so the test found no decision to compare. The same test passed on the PR's run.
   - Options: fix it in this phase's PR; a separate PR first; a follow-up.
   - Answer: **fix it in this PR** (owner). After the burst, one more request with the same body must answer 200. Every 200, the burst's and that one's, must be the same decision, with exactly one row and one attempt. The test still checks I1 under contention, without needing a burst request to finish within 60 ms on a busy runner.
4. **O24: how 2b runs LightGBM on this Mac.**
   - Options: only inside Docker; the owner installs libomp; decide in Phase 7.
   - Answer: **only inside Docker** (owner). Anything that imports LightGBM (training, the backtest, shadow scoring) runs in this repo's image, and nothing on the Mac imports it. Whether the image needs Debian's OpenMP runtime (`libgomp1`) is checked in Phase 7. The local safety guard blocks running this repo's image by hand, so it couldn't be checked here. This closes O24.

### D52. Replay and loader choices (2026-11-03)

Choices made at the start of Phase 6, within D32, D33, D36, D37, D40p, D40u and D40v, and shown to the owner with D51's questions. No doc changes.

| # | Choice | Value | Why |
|---|---|---|---|
| a | Where `make parity` runs | its own Compose project, `risk-engine-parity`, started empty and removed with its volumes at the end; the normal stack must be stopped first, since both use the same ports. Its stub runs with `STUB_REPLAY=1` and a relay interval of 0 | the replay's wait needs a watermark behind the first replayed time, as on a fresh stack; the relay delay changes no feature value, and Phase 6 criterion 4's test covers a delay above 0 |
| b | Where CI's I7 test runs | on the normal stack: it waits until the consumer has caught up with the topic, empties the stack's Redis as `tests/adversarial/test_rebuild.py` does, and replays through a replay-mode stub run in the test process against the stack's API and consumer. Each run maps PaySim names in a new UUID namespace | the watermark must be behind the replayed times; a new namespace keeps each run's keys and accounts apart from earlier runs' rows in Postgres |
| c | The parity sample | the first 10,000 loaded transfers in time order | D36's contiguous sample of about 10k |
| d | Each transfer's Idempotency-Key | the UUID v5, in the loader's namespace, of the row's step and its line in the CSV | deterministic, and inside tally's key pattern (D1) |
| e | What the loader refuses | a file whose steps go backwards; an hour with more transfers than milliseconds; a transfer whose sender is its receiver | times must strictly increase for the wait to be exact (D36): with two transfers in one millisecond, the watermark could pass the second's time before its event was applied. The ledger refuses a transfer to the same account; the profile counts such rows first |
| f | A zero amount | a row whose amount rounds to 0 cents | the ledger refuses an amount below 1 cent, so 0.004 can't be posted either (D40p) |
| g | The replay time header | in replay mode, `X-Replay-Time` is required, an ISO-8601 time with an offset, truncated to the millisecond; without a valid one the stub answers 400. Normal mode ignores the header | a replayed transfer never falls back to the wall clock |
| h | When the replay stops | on any answer but 201; on a 201 whose `createdAt` isn't the replay time, which means the stub isn't in replay mode; when the watermark hasn't reached the previous transfer's time within 30 s. It refuses to start when the watermark is already at or past the first transfer's time | every replayed transfer is decided and applied before the next (D36), or the run says why not |
| i | Dependencies | none added: the loader, the profile and the offline pipeline use Python's own csv, decimal and uuid modules | Phase 6 trains nothing; pandas (D40s) waits until a phase needs it |
| j | The profile's report | `ml/reports/paysim_profile.md`, written by the profile script, naming the CSV's SHA-256 | a committed report, traceable to one file and one script (D29) |
| k | Which rows share an hour's spacing (D33) | the transfers the loader keeps, TRANSFER and CASH_OUT rows of at least 1 cent, in file order; the other types take no slot | the loader's output is evenly spaced, whatever else the hour holds |
| l | Where the hour of day (D35) is computed | `risk/features/compute.py`, beside the 31 features | the offline pipeline now and the shadow scorer in Phase 8 read it from one place (CLAUDE.md) |

### D53. The burst test can't promise one attempt (2026-11-03)

- Found while fixing the burst test (D51.3): design.md section 4 lets an insert commit after the 60 ms deadline has cut its request off. That request answers 503 and records no attempt (3.3 records one only after a 200 for a new decision). When the whole burst misses the deadline, the one row can therefore exist with no attempt, and the follow-up request only replays it. "Exactly one attempt" can fail in exactly the case D51.3 set out to handle.
- Options: assert at most one attempt; also let a replay re-record its stored attempt, a change to design.md 3.3; leave the test as it was.
- Answer: **at most one attempt** (owner). This replaces "and one attempt" in D51.3's answer; the rest of D51.3 stands. The burst test keeps exactly one row and the same decision in every 200. `tests/integration/test_attempts.py` still checks that a new decision answered 200 adds exactly one attempt, and that 8 concurrent duplicates add one.
- Consequences: an insert that commits after the deadline leaves its attempt missing until `make rebuild-features` restores it. The Phase 6 PR lists this under Follow-ups.

### D54. What the PaySim profile settled (2026-11-04)

Phase 6's stop-and-ask, answered after `make profile` wrote `ml/reports/paysim_profile.md` from the owner's CSV (SHA-256 `16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b`). The owner approved the doc changes below before they were made.

1. **The sample (D32).** TRANSFER and CASH_OUT over all days load 2,770,393 transfers, 8,197 of them fraud. Senders almost never repeat (2,766,838 of 2,768,614 senders send once), so the sender-side feature families are almost always empty and only the receiver's inflows (`dest_in`) often have members.
   - Options: confirm as planned; re-profile with every type.
   - Answer: **confirm as planned** (owner). The model keeps all 31 features: empty ones cost the model nothing and are what the live guard computes. The model card names the sparse sender side as a limitation.
2. **The starter rules on PaySim.** Rule set v1 would hold 97.15% of the loaded transfers, nearly all through `large_transfer`, since its thresholds are CAD cents (D20) and PaySim's amounts are larger. A backtest at the rules' own hold rate (D40q) would compare nothing.
   - Options: add a PaySim rule set, rescaled to hold about 1%, in this PR; the same in Phase 7; keep v1 only.
   - Answer: **add a PaySim rule set in this PR** (owner). `ml/rules/paysim.yaml`, version 2, written by `make paysim-rules`: D20's six rules, `default: allow`, `on_timeout: hold`, with every threshold on a cent feature (`amount` and the sums) multiplied by one integer factor and every count threshold unchanged. The factor is the smallest integer at which the set holds or blocks at most 1% of the training days' transfers (D34, D38); a hold and a block both stop the money, so both count. Only the backtest reads the file; the service still loads `risk/rules/ruleset.yaml`. Its report is `ml/reports/paysim_rules.md`.
   - Why: PaySim's currency is unnamed, so one factor changes the scale of the money and nothing else. Choosing it on the training days keeps the test days unseen.
   - Consequences: on PaySim the set acts mostly as "hold the largest amounts", because the sender-side rules can hardly fire (point 1). D38's day split is built now, in `ml/split.py`, and Phase 7 reuses it.
3. **The hour of day as a model input (D35).** The profile's hour table shows fraud spread almost evenly over the 24 hours while other transfers nearly stop at night, so the hour alone would separate much of the fraud. That is how PaySim simulates fraud, not something a real guard could count on.
   - Options: drop the hour; keep it and name it in the model card; train with and without it and report both.
   - Answer: **drop the hour** (owner). This replaces "plus the hour of day of `requestedAt`" in D35's answer; the rest of D35 stands. The model's inputs are the 31 features of D19.
   - Why: the same reason D35 gives for leaving out the balance columns: it makes PaySim artificially easy.
   - Consequences: the hour stays in the offline pipeline, computed in `risk/features/compute.py` (D52l), for the profile's hour table (D51.2); no model reads it.
- Doc changes: design.md section 8 (offline pipeline, a new PaySim rule set entry, shadow, backtest); requirements.md US13.2 and US15.1; architecture.md section 6; phases.md Phase 6 scope and a new criterion 10, and Phase 7 criterion 2.

### D55. Phase 7's opening questions (2026-11-07)

Five questions, asked before any Phase 7 code: D30, which Phase 0 deferred to here, three the docs didn't settle, and the check D51.4 left to this phase.

1. **D30: LightGBM under CLAUDE.md's AI-model rule.**
   - Question: may this repo train a LightGBM model from the owner's PaySim copy and save it? CLAUDE.md says never to download or run an AI model.
   - Options: allowed; a logistic regression written here in numpy, with no machine-learning library; no model.
   - Answer: **allowed** (owner). LightGBM is a code library, installed through uv with D41's safeguards like every other dependency; no pretrained model is downloaded. The model is trained here, from the owner's CSV, and only inside Docker (D51.4). This closes D30 and meets Phase 7's criterion 1.
2. **What builds the feature array.** LightGBM takes the training days' features as one numeric array.
   - Options: numpy; pandas, as D40s planned.
   - Answer: **numpy** (owner), added to D40s's 2b list as a direct dependency. LightGBM depends on it already, so it adds no package to the lock. pandas stays on the list and is not installed. The rest of D40s stands.
   - Consequences: the lock adds lightgbm 4.7.0 and numpy 2.5.4, and LightGBM's own dependencies scipy 1.18.1 and narwhals 2.27.1. Each has a wheel for macOS arm64 and Linux arm64 for Python 3.12, older than D41's cutoff.
3. **A tie at the 1% line.** A tree model gives many transfers exactly the same score, so a tie can straddle the line, and no line then holds 1% to within one transfer.
   - Options: never above 1%; closest to 1%; break ties by transfer id.
   - Answer: **never above 1%** (owner). Of the threshold days' n transfers, k = ⌊n / 100⌋ may be held. The threshold is the lowest score at which at most k of them score at or above it, and a transfer is held when its score is at or above the threshold. Without a tie at the line exactly k are held, within one transfer of 1%; with one, fewer, and the model card gives the count.
   - Why: review capacity is a ceiling, and the PaySim rule set already holds at most 1% (D54).
   - Doc changes (owner approved): requirements.md US13.4, design.md section 8 (split and threshold), phases.md Phase 7 criterion 4.
4. **Whether the model file is committed.**
   - Options: commit it; keep it out of git and rebuild it with `make model`.
   - Answer: **commit it** (owner). It holds trees: the features they split on, the split values and the leaf values. It holds no PaySim row and no account name, since its inputs are the 31 features. The model card names its SHA-256.
   - Why: Phase 8, and anyone who clones the repo, use exactly the model the card describes.
5. **LightGBM's OpenMP runtime in the image** (D51.4).
   - Found: in the image built from `python:3.12.16-slim-trixie`, `import lightgbm` fails with `OSError: libgomp.so.1: cannot open shared object file`.
   - Options: install Debian's `libgomp1` with apt, pinned; the same, unpinned; don't add it.
   - Answer: **apt, pinned** (owner). The image's final stage installs `libgomp1=14.2.0-19`, trixie's version, built from gcc-14 like the base image's C runtime, without recommended packages, and deletes the package lists. If Debian replaces that version, the build fails, and a PR moves the pin. Nothing is installed on the Mac.

### D56. Training choices (2026-11-07)

Choices made within D34, D38, D51.4, D54 and D55, shown to the owner with D55's questions and accepted.

| # | Choice | Value | Why |
|---|---|---|---|
| a | Where training runs | `make model` runs a `model` Compose service from the app image, in a Compose profile so `make up` never starts it; it reads `data/` read-only and writes only `ml/model/` and `ml/reports/` | LightGBM runs only inside Docker (D51.4) |
| b | What it writes | `ml/model/model.txt` (LightGBM's text format), `ml/model/model.json` (the model's name, its 31 inputs in order, its threshold, the CSV's SHA-256 and the days of each set) and `ml/reports/model_card.md`. The name is `lgbm-` and the first 12 hex digits of `model.txt`'s SHA-256 | Phase 8 loads one folder, and the name changes whenever the model does |
| c | Settings | fixed, no search: binary objective, 300 rounds, learning rate 0.05, 31 leaves, at least 100 transfers per leaf, a fixed seed, one thread, deterministic mode; no early stopping, no class weights | early stopping needs a held-out set, and the threshold days only set the threshold while the test days stay unseen (D38); one thread makes two runs identical |
| d | The threshold | D55.3 on the threshold days' scores | review capacity is a ceiling |
| e | Metrics on the test days | the threshold from the threshold days, unchanged; the card also gives the share it holds on the test days. PR-AUC is average precision, computed in this repo with numpy | the test days stay unseen until scored; no further library |
| f | Tests | no test on the Mac imports LightGBM. One integration test trains twice on generated data inside the image and checks that the two model files are identical | every test runs on the Mac and in CI, none skipped, and determinism is checked in CI |

### D57. Phase 8's opening questions (2026-11-07)

Five questions the docs didn't settle, asked before any Phase 8 code.

1. **A slow model.** D39 scores in the request, before the row is stored, under the 60 ms deadline (design.md section 4). A model slow enough would push the decision past the deadline into a 503, so the model would change what the caller gets, which I8 forbids.
   - Options: a 10 ms limit, with the model scored on its own worker thread; a 5 ms limit; no limit, scoring inline, which drops I8's slow-model case (a change to design.md).
   - Answer: **a 10 ms limit** (owner). Scoring runs on one worker thread of the API; past 10 ms the row stores no score, the timeout is counted, and the decision goes on. A request whose wait has ended is dropped from the thread's queue before it runs, so a slow model can't pile up work. The limit is `SHADOW_TIMEOUT_MS`, default 10, in design.md 1.6.
   - Consequences: a decision waits at most 10 ms for the model, inside its 60 ms deadline.
2. **What the bench runs.** Phase 8 asks for the bench with shadow off and on, and for p99 within 50 ms with shadow on at every rate below the one where 2a's p99 crossed 50 ms. The committed results are from 2026-11-03, before D51.1 changed the consumer's loop, which D51.1 left for Phase 8's bench to measure.
   - Options: every scenario again in one session on the phase's final commit (direct with shadow off and on, then stub and hot with shadow on); direct only, off and on, with README keeping the 2026-11-03 stub and hot tables; direct with shadow on only, compared with the 2026-11-03 run.
   - Answer: **every scenario again, in one session** (owner). The shadow-off direct run is the baseline: the rate where its p99 first passes 50 ms is "the one where 2a's p99 crossed 50 ms" in Phase 8's criterion 4, and the model's added latency is shadow on minus shadow off at each rate, from the report script. All of README's tables then come from one run on one commit. design.md 12 now says so.
3. **"The model at 1%" in the backtest** (D40q).
   - Options: the test days' own 1% line; the threshold in `model.json`, set on the threshold days; both.
   - Answer: **the test days' own 1% line** (owner): of the test days' n transfers, the model holds those scoring at or above the lowest score at which at most ⌊n / 100⌋ do, D55.3's rule, the same method that matches the rules' own hold rate. The model card already reports the shipped threshold on the test days.
4. **How README's 2b numbers come from a script** (D29).
   - Options: a block written by `make report` from the backtest's data file and the bench results, checked by `make check-readme` in CI; links to the reports, quoting no numbers.
   - Answer: **a block written by a script** (owner). The backtest writes `ml/reports/backtest.json` beside its report; `make report` writes README's 2b block from it and from the bench results, and `make check-readme` fails if the block differs.
5. **Shadow on by default.**
   - Options: on, the API loading the committed model unless `SHADOW_MODEL_DIR` is empty; off unless set.
   - Answer: **on by default** (owner). `make up`, CI and every test run with the committed model; `SHADOW_MODEL_DIR` set to empty turns shadow off, as the bench's shadow-off run does. design.md 1.6 now names the setting.

### D58. Shadow and backtest choices (2026-11-07)

Choices made within D39, D40q, D51.4, D55, D56 and D57, shown to the owner with D57's questions and accepted. The owner approved the doc changes with them: design.md 1.6 (two settings), section 2 (the shadow columns' checks), section 8 (shadow) and section 12 (shadow cost).

| # | Choice | Value | Why |
|---|---|---|---|
| a | Where the model comes from | `ml/model/` is copied into the image; at startup the API loads `model.json` and `model.txt` from `SHADOW_MODEL_DIR`, default `ml/model` | the image carries exactly the model the card describes (D55.4) |
| b | Load checks | `model.json` readable; its inputs exactly the 31 features in `FEATURE_NAMES` order; `model.txt`'s SHA-256 equal to `model.json`'s; LightGBM's feature names equal to the inputs. A failure is logged and the API starts without a model | design.md section 7: a missing or corrupt model leaves decisions unchanged |
| c | What a row stores | shadow off: both columns null. A model loaded: its name always, and the score null when scoring fails. Shadow on but no model loaded: both null | the name tells "off" from "failed" whenever a model loaded; the counter and the startup log tell the rest |
| d | A valid score | one finite number from 0 to 1, the range of LightGBM's binary objective; anything else is stored as null and counted | makes I8's "nonsense" exact |
| e | Migration | `0005_shadow.sql`: `shadow_score double precision` and `shadow_model text`, both nullable, with checks that a score is null or from 0 to 1 and that a score has a model name | a row can't hold a score that no model gave |
| f | Counter | `risk_shadow_failures_total`, by `reason`: `unavailable` (shadow on, no model loaded), `error`, `invalid`, `timeout` | design.md section 7's shadow error counter |
| g | Replays | not scored again; a replay returns the stored row as before | one row, one score |
| h | Scoring | one worker thread; the 31 features as float64 in `model.json`'s order; LightGBM predicts with one thread | float32 can't hold every amount in cents (ml/dataset.py); one OpenMP thread suits a single row |
| i | I8 test | LightGBM can't load on the Mac (D51.4), so the test runs the decision path in the test process against the stack's Redis and Postgres with stand-in models (none, a working one, one that raises, one that returns nonsense, one that's slow) on identical Hypothesis-generated histories in separate accounts. A second check sends the same requests to the stack's API, with the real model, and to a second API with shadow off | the stand-ins cover every failure; the second check covers the real model |
| j | `tests/integration/test_shadow.py` | rows from the stack's API hold `model.json`'s name and a score equal to LightGBM's score of the stored features, recomputed in the image; a second API started with a corrupt model stores null scores and counts them; a model that raises stores null and is counted | Phase 8's criterion 2 against the real model |
| k | `make backtest` | runs `python -m ml.backtest` in the image (D51.4), in a Compose service like `model`'s; reads the CSV, `ml/rules/paysim.yaml` and `ml/model/`; refuses a CSV whose SHA-256 differs from the one the model and the rule file name; writes `ml/reports/backtest.md` and `ml/reports/backtest.json` | the comparison runs on the data the model and the rule set were built from |
| l | Talk and video | `docs/talk.md` gains a 2b part; the 3-minute demo video is the owner's to record (PLAN.md section 9) | |
| m | Dependencies | none added | |

### D59. A tie at the backtest's line (2026-11-07)

- Found by the first `make backtest` on the owner's CSV: on the test days' 67,944 transfers, 659 score above 0.03012039931478348 and 199 score exactly that, 57 of them fraud. The rules hold 786 and 1% is 679, so both lines fall inside the tie. By D55.3's rule, which D57.3 applied to the backtest, the model then held 659 at both lines, and US15's comparison at the same hold rate wasn't one: the model held 127 fewer transfers than the rules.
- Options: show the model on both sides of a tie at the line; split the tie in proportion (the tied group's counts scaled by the share of it needed, what a random pick gives on average); keep D55.3's rule and name the tie in the report; break the tie by file order.
- Answer: **both sides of the tie** (owner). At each line, when the transfers sharing the score at the line straddle the count, the backtest reports the model holding those above the tie and the model holding the tie as well, and how many share the score; otherwise one line, holding exactly the count. Nothing is guessed and no tie is broken. This replaces "the model holds those scoring at or above the lowest score at which at most ⌊n / 100⌋ do, D55.3's rule" in D57.3's answer, and the same for the rules' rate; the rest of D57.3 stands. D55.3 still sets the model's threshold.
- Doc changes (owner approved): design.md section 8 (backtest), requirements.md US15.1, and phases.md Phase 8 criterion 3 to match.

### D60. A bench the host can't spoil (2026-11-08)

- Found by Phase 8's first full `make bench`: k6 ran the shadow-on direct run's 800/s step for 1480.8 s instead of 180 s, because the Mac slept with its lid closed during it. The whole session ran on battery, and the Mac was in use during the shadow-off run. The report accepted all of it, since it checked only the planned step length. A second session, on battery but idle and awake, put the shadow-off crossing back at 2a's rate and the shadow-on crossing a step lower. A shorter check on AC power, with today's code and shadow on, then held 800/s within 50 ms. So power, not the code, is the first suspect, and the bench has to rule it out.
- Options: a report that refuses a step k6 ran for more than 5% longer or shorter than its length, and a run on battery, with `make bench` refusing to start on battery; only the run-time check; no change, with care taken by hand.
- Answer: **refuse both** (owner). Each step's file records whether the host was on AC power at the step's start and end (`pmset -g batt`, built into macOS), and each scenario's conditions file at its start. The report refuses a step or a scenario on battery, and a step whose k6 run time is more than 5% off its length; an overloaded step runs a little long while k6 waits for its last requests, which 5% allows. `make bench` won't start on battery. The conditions line in README names the power source when it is known. The bench runs on AC power, with the lid open and the Mac otherwise idle, under `caffeinate -i`, which is built into macOS and ends with the command.
- Doc change (owner approved, with D61): design.md section 12's recording bullet names the power source and the two refusals.

### D61. A stop the bench confirms, and the stub path with the model off (2026-11-08)

- Found by Phase 8's third full `make bench`, the first on AC power with the Mac idle and kept awake, and every step within 5% of its length. With shadow on, the direct run held 800/s (p99 10.2 ms) and crossed at 1,600/s, as 2a's did, so the battery run's failure at 800/s didn't come back (D60). But the shadow-off direct run stopped at 200/s (p99 92.7 ms, errors 1.03%) after holding 100/s at a p99 of 5.1 ms, with its slowest request at 0.74 s: a stall of a second or two, not a limit. One 3-minute step decides where a scenario stops, so one stall moved criterion 4's baseline. Separately, the stub run with shadow on crossed at 400/s (p99 257.8 ms) in this run and in the battery run, where 2a's held 400/s at a p99 of 7.3 ms. Shadow scoring and D51.1's consumer loop have both changed since 2a, and D57.2 runs the stub path with shadow on only, so the results can't tell the two apart.
- Options: run a step again when it passes a limit, and add a stub run with shadow off; only run steps again; neither, and run the session again.
- Answer: **both** (owner).
  1. When a step passes a limit (p99 over 50 ms, errors over 1%, or k6 dropping more than 1% of its planned requests), or a hot step's p99 passes 50 ms, the bench runs the same rate or window size once more, straight away. The scenario stops only if the repeat passes a limit too; otherwise it goes on to the next rate or size. Both attempts are kept and README's tables show both. The repeat decides its rate: in the crossing, in the shadow cost and in criterion 4's check. The report keeps out of README a scenario whose last step stopped it without a repeat, as a run from before D61, or one cut off between the two attempts, would have. This replaces "doubling until p99 passes 50 ms or errors pass 1%" in D40o and the stop rule in D48.2 and D49 row a; the rest of each stands.
  2. The session runs the stub scenario with shadow off, `stub-off`, after direct and before stub with shadow on. README's 2b block adds the shadow cost through the stub, on minus off at each rate, beside the direct path's. This adds to D57.2's answer; the shadow-off direct run stays criterion 4's baseline.
- Why: a real limit fails twice in a row, while a stall of the host fails once. The stub runs with shadow off and on, on one commit, differ only in the model, so README can show whether the stub path's drop comes from the model.
- Consequences: each scenario runs one more step at its stop, and the session one more scenario. The repeat starts straight after the first attempt, as each doubling does, with no time to settle: in the run above, the consumer's offset lag was 0 at the end of every step, the stub's 400/s step included.
- Doc changes (owner approved): design.md section 12's decision latency, hot account and shadow cost bullets, and its recording bullet with D60's change.
- Outcome: the session in `bench/results/2026-11-09/`, on commit `12eb438`, on AC power, every step within 5% of its length. Direct crossed at 1,600/s with shadow off and on, the stub path at 800/s with both, and the hot account at 10,000 events; every crossing failed on its repeat, and no step held on one. The stub path held 400/s with shadow off (p99 8.6 ms) and on (12.0 ms), so the earlier failures there didn't come back, and it crosses where 2a's did, with D51.1's consumer loop and with the model. Below its crossing, the stub path's feature lag p50 is from 104 to 117 ms, against 2a's 364 to 376 ms: D51.1's change cost no capacity.

Choices made while building D61. Row b was found in self-review and goes to the owner with this change. No doc changes.

| # | Choice | Value | Why |
|---|---|---|---|
| a | A repeat's files | `<step>-repeat.json` and `<step>-repeat-stack.json` beside the first attempt's; each step's file records its attempt, 1 or 2 | the report reads both attempts and orders them; results from before D61 read as first attempts |
| b | Criterion 4's check | a rate below the model-off crossing is within budget only if the model-on step there held: p99 within 50 ms, and errors and dropped requests within 1%. Before, a step that stopped for errors counted whenever its p99 was within 50 ms | the report can't call a rate within budget where the model-on run stopped |

## Open questions

None. Every question from PLAN.md section 5 and every question found since is answered above.
