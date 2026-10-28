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
| r | Redis persistence | not relied on: windows are rebuilt by replaying `ledger.entries` from the time watermark − 24 h − 5 min, and attempts from the decision log | Redis loss is a failure mode with a procedure, not data loss |
| s | Libraries | runtime: fastapi, uvicorn, pydantic, redis, psycopg, confluent-kafka, httpx, pyyaml; dev: pytest, hypothesis, ruff, mypy, pip-audit; 2b: lightgbm, pandas | widely used; each pinned and justified in the PR that adds it |

## Open questions

None. Every question from PLAN.md section 5 and every question found since is answered above. One answer is a deferral the owner chose: D30 (the AI-model rule) is confirmed at the start of Phase 7.
