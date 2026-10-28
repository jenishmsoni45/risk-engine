# Decisions

Every question asked in Phase 0 and its answer, then every later decision. Each entry gives the question, the options considered, the answer, why, and what it changes. Entries are never edited after the fact; a changed decision gets a new entry that names the one it replaces.

Status (2026-10-28): tally's side of the decision call is built and merged (D13). Phase 0 resumes with the open questions at the end of this file.

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

## Open questions

To settle when Phase 0 resumes, before the Phase 0 docs are written.

Features
- O1 (PLAN Q10). Which time drives the windows (`createdAt` or `effectiveAt`), and is the decision time t the request's `requestedAt`?
- O2 (PLAN Q5). How late may an event be, and what happens to one later than that?
- O3 (PLAN Q2). Exact windows (a sorted set per account) or bucketed counters?
- O4 (PLAN Q9). Which entry kinds feed the features, and does a reversal take back its original?
- O5 (PLAN Q1). How are bursts of attempts that haven't committed yet counted?
- O6. The feature catalog: which accounts (sender, receiver) and directions (in, out) get windows, plus attempt features and request fields.

Rules and the decision path
- O7 (PLAN Q7). Which starter rules ship in 2a, and what each one catches.
- O8. Rule semantics: evaluation order, how fired actions combine, allowed operators, unknown feature names, loading and versioning.
- O9 (rest of PLAN Q3). When its own parts (Redis, Postgres) are slow or down, does the service fail fast with 503 and store no row?
- O10. Behaviour under consumer lag: keep deciding on stale features, or fall back?

Stub, fixture and integration
- O11. The stub's scope (endpoints, idempotency, balances) and whether it copies tally's relay delay.
- O12. How the fixture is captured from real tally after its new phase.
- O13. How this repo runs real tally for the local end-to-end check (a local clone and `bootRun`, or a build in Docker), and how the risk engine's containers reach tally's Kafka.

2b
- O14 (PLAN Q12). Which PaySim sample, and the check of how often accounts repeat.
- O15. PaySim steps are whole hours: how replayed transfers get times within an hour.
- O16 (PLAN Q13). The review capacity (share of transfers held) that sets the model's threshold.
- O17 (PLAN Q14). Which features the model may use.
- O18. How the replay reaches exact parity for I7 (the online state must have applied every event before t when it decides).
- O19. What CI runs for I6 to I8, given PaySim can't be committed.

Project rules and environment
- O20. CLAUDE.md allows a number in README.md or docs/ only if a repo script produced it: do decided targets, thresholds and config values count?
- O21. CLAUDE.md says never run an AI model: confirm that a LightGBM model trained here from PaySim is allowed (the rule covers downloaded or pretrained models).
- O22. `uv` is in PLAN.md's Install row but not installed on this Mac. It is needed from Phase 1.
- O23. Compose: image tags, host ports that don't clash with tally's, network exposure, and whether the decision API has any authentication.
