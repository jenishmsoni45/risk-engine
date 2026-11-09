# Requirements

The risk engine decides allow, hold or block for each transfer the ledger (tally, project 1) is about to post, within a latency budget, from rules over rolling per-account features built from the ledger's event stream. 2b adds a LightGBM model trained on PaySim and scored in shadow next to the rules.

Every requirement here traces to an entry in [decisions.md](decisions.md) (D-numbers) or to PLAN.md. The contract with tally is [tally-integration.md](tally-integration.md). Thresholds, targets and configuration values are decisions (D29); measured results appear in README.md only when a script in this repo produced them.

## 1. Scope

- **2a** (Phases 1 to 5, Nov 16 to 29): a decision API that tally calls before it opens its posting transaction; a hard caller timeout with a per-rule-set backup answer; rolling per-account counts and sums over 1 min, 1 h and 24 h, kept in Redis and correct under duplicate, reordered and late events; versioned YAML rules; a decision log that explains every decision; a ledger stub for CI; load, fault and end-to-end tests with published numbers.
- **2b** (Phases 6 to 8, Feb 1 to 21): PaySim replayed as ledger transfers; one feature module shared by the online service and the offline pipeline, with point-in-time and parity tests; a LightGBM model with a time-based split and a threshold set by review capacity; shadow scoring beside the rules; a backtest CLI.

## 2. Users

| User | Who | What they need |
|---|---|---|
| The ledger | tally (or the ledger stub in tests), calling before it posts a transfer | A decision within its timeout, the same answer for a retried Idempotency-Key, and a backup answer when there is none |
| Risk analyst | Writes and versions rules, investigates decisions | Plain rules, a complete reason for every decision, and evidence that a change is better, not just stricter |
| On-call engineer | Runs the service | Known behaviour when Redis, Postgres, Kafka or the service itself fails, and published latency, timeout and lag numbers |
| ML engineer (2b) | Trains and evaluates the model | Training features equal to what the service saw, no leakage, a threshold tied to review capacity, and a shadow mode that can't change a live decision |

## 3. User stories and acceptance criteria

Each criterion is checked by a test or a script named in [phases.md](phases.md). "The caller" is tally or the ledger stub; the stub behaves as tally does (D24).

### 2a

**US1. Decide before posting.** As the ledger, I want a decision for a transfer before I post it, so that risky transfers never move money.
1. `POST /v1/decisions` with the request of tally-integration.md 3.1 answers 200 with `decisionId` (a UUID v4), `decision` and `onTimeout` (each `allow`, `hold` or `block`, lowercase), `ruleSet` (`"v<version>"`) and `fired` (a list of rule ids) (D7, D13, D40f).
2. A 200 body is at most 16 KiB (D13).
3. A request with a missing or malformed field, a non-positive amount, the same account on both sides, a currency that isn't 3 uppercase letters, or a `requestId` outside `[\x21-\x7E]{1,255}` gets a 400 problem and no decision row (D1).
4. The server keeps an idle connection open for at least 5 s (D13; configured to 10 s, D40e).

**US2. One key, one answer.** As the ledger, I want a retried transfer to get the same answer, so that a retry never turns a hold into an allow.
1. I1: the same `requestId` with the same `fromAccountId`, `toAccountId`, `amount` and `currency` always returns the same `decisionId`, `decision`, `ruleSet` and `fired`, including when the duplicates arrive concurrently; `requestedAt` is not compared (D11, D13).
2. The same `requestId` with a different transfer gets a 409 problem and changes nothing (D11).
3. A replayed decision carries the current rule set's `onTimeout` (D13).

**US3. Never hang.** As the ledger, I want a bounded wait and a defined backup answer, so that an outage of the risk engine can't stall payments.
1. I4: when no valid 200 arrives within 100 ms (the service paused, refusing connections, answering slowly, answering a non-200 or a body that breaks the contract), the caller applies the backup answer, and its call ends within 100 ms plus the 100 ms test margin (D4, D40m).
2. The backup answer is the `onTimeout` of the latest valid 200, and the caller's configured default, `hold`, before any (D10).
3. A backup `hold` or `block` answers the customer 503 `/problems/risk-unavailable` and posts nothing; a backup `allow` posts with `riskCheck: fallback` and `decisionId: null` (D6, D7).
4. When Redis or Postgres is slow or unavailable, the decision API answers 503 within its 60 ms internal deadline and stores no decision row (D22, D40d).

**US4. Hold and block refuse.** As the ledger, I want hold and block to refuse the transfer in a way the customer and support can tell apart, so that nothing moves without a decision.
1. `hold` gives 403 `/problems/transfer-held` and `block` gives 403 `/problems/transfer-blocked`, each with the `decisionId` and without the fired rules; nothing is posted and no event is published (D3).
2. `allow` posts the transfer, and its event carries `decisionId` and `riskCheck: decided` (D7).

**US5. Explain every decision.** As a risk analyst, I want every decision stored with its reason, so that I can answer "why was this held?" from one row.
1. I5: every decision row stores the rule set's version and file hash, the fired rules (an empty list when none fired), a snapshot of every feature the rules saw, and the feature lag; the schema refuses a row without them, and a test checks a stored row (D23, D27).
2. The snapshot holds exactly the 31 features of D19, with the values the rules were evaluated on.

**US6. Features that don't lie.** As a risk analyst, I want rolling features that are right whatever the stream does, so that rules fire on facts.
1. Each decision sees the 31 features of D19, with window membership `t − W ≤ time < t` (D14, D40a, D40b).
2. Transfers and multi-line entries count, netted per account per entry; a net of zero adds nothing; reversals are ignored (D17).
3. I2: duplicate and reordered events never change a feature value; checked against a brute-force oracle on generated event sequences.
4. I3: at any query time within the retained range, every window value equals a recompute from the raw event log.
5. An event applies whenever it arrives if `createdAt` is no older than watermark − 24 h − 5 min; an older one is dropped from the windows and counted (D15, D40c).
6. A message that fails parsing or validation is stored in `bad_events`, counted, and its offset committed, and the partition keeps flowing (D40i).

**US7. Catch bursts before they commit.** As a risk analyst, I want attempts counted the moment they are asked about, so that a fast burst or refused probing is visible.
1. Every new decision adds one attempt for its sender, held and blocked ones included; a retried `requestId` adds none (D18, D40g).
2. Attempt features equal a recompute from the decision log.

**US8. Plain, versioned rules.** As a risk analyst, I want rules in a small YAML file with a version, so that changes are reviewable and every decision says which rules made it.
1. The rule file has `version` (an integer), `default` (`allow`), `on_timeout` (`allow`, `hold` or `block`) and `rules`, each with a unique `id`, an `action` (`hold` or `block`) and `all`, a non-empty list of `{feature, op, value}` conditions with `op` one of `>`, `>=`, `<`, `<=`, `==`, `!=` (D20, D21).
2. The strictest fired action wins (block over hold over allow); with none fired, the decision is `default`; `fired` lists every fired rule in file order (D21).
3. A file that doesn't parse, breaks the schema, repeats an id or names an unknown feature stops the service at startup (D27).
4. The starter rule set is D20's six rules with `on_timeout: hold` (D5, D20), and a table of rule fixtures produces the expected decisions.

**US9. Join decisions to money.** As a risk analyst, I want each posted entry linked to the decision that allowed it, so that I can follow an allow to the money it moved.
1. Every event with `riskCheck: decided` names a `decisionId` that exists in the decision log (D7).
2. The stub's events parse exactly as the fixtures captured from real tally do (D25).
3. `make e2e` runs real tally from the owner's clone against this service and checks the join for every transfer it posts (D26).

**US10. Know how it behaves.** As an on-call engineer, I want measured numbers and rehearsed failures, so that I know what happens before it happens.
1. `make bench` produces p50 and p99 decision latency at each stated rate, the rate where p99 passes 50 ms, the timeout rate and the consumer lag, into `bench/results/` with the run's conditions (D40o).
2. Fault tests cover the decision API, Redis, Postgres and Kafka paused, and the consumer killed mid-batch; each ends in the behaviour design.md's failure table states.
3. The decision API, the consumer and the stub serve `GET /metrics` (D40t, D40z).

### 2b

**US11. Replay PaySim as ledger transfers.** As an ML engineer, I want PaySim turned into ledger transfers, so that the model learns from the same features the service computes.
1. A profiling script reports, for the owner's PaySim copy, rows and fraud per type and how often senders and receivers repeat; its output confirms the sample before features are chosen (D32).
2. The loader takes TRANSFER and CASH_OUT rows, maps accounts, amounts and currency as D40p states, and spaces each hour's rows evenly in file order (D33).
3. The replay posts through the stub and waits for each event to be applied before the next decision (D36).
4. Nothing from `data/` is committed.

**US12. One feature module, point in time.** As an ML engineer, I want offline features computed by the same code as online ones and only from the past, so that training matches serving.
1. The online path and the offline pipeline import one feature module; no feature logic is duplicated (CLAUDE.md).
2. I6: a feature for a decision at t never uses an event at or after t; a property test injects future events and sees no change.
3. I7: on the replayed sample, offline features equal the features logged online, exactly; locally on about 10k real PaySim transfers, and in CI on generated PaySim-format data (D36, D37).

**US13. Train a model honestly.** As an ML engineer, I want a model trained without leakage and thresholded by review capacity, so that its numbers mean something.
1. Phase 7 starts only after the owner confirms that training LightGBM here is allowed under CLAUDE.md's AI-model rule (D30).
2. The model uses only the 31 features: D35's inputs without the hour of day (D54).
3. The split is by days: test on the last 7, set the threshold on the 2 before, train on the rest (D38).
4. The threshold holds at most 1% of transfers on the threshold days (D34, D55).
5. One command rebuilds the model card: data, split, inputs, threshold, PR-AUC, recall and fraud value caught at the 1% hold rate, false-positive rate, and limitations.

**US14. Shadow without risk.** As an on-call engineer, I want the model scored next to every decision without any way to change it, so that we learn safely.
1. I8: decisions are identical with shadow scoring on and off, for the same requests and features.
2. The score is stored in the decision row; a scoring error stores no score and the decision is unaffected (D39).
3. With shadow on, p99 stays within 50 ms at every bench rate below the one where 2a's p99 crossed 50 ms, and the model's added latency is published.

**US15. Backtest rules against the model.** As a risk analyst, I want rules and model compared on the same test days at the same hold rate, so that "better" means better.
1. The backtest CLI reports, on the test days, the PaySim rule set (D54) at its own hold rate against the model at that same rate, plus the model at 1%, with recall, fraud value caught and false-positive rate for each (D40q). Where transfers sharing the model's score at a line straddle that rate, the model is reported on both sides of the tie (D59).

## 4. Non-functional requirements

| Area | Requirement | Source |
|---|---|---|
| Latency | Decision p99 within 50 ms, measured at the client, up to a stated rate; the bench reports the rate where p99 crosses 50 ms | D4, D40o |
| Caller timeout | 100 ms for the whole call, after which the backup answer applies | D4 |
| Internal deadline | The decision API answers within 60 ms or returns 503 | D22, D40d |
| Failure policy | Fail closed by default (`on_timeout: hold`), set per rule set | D5, D10 |
| Correctness | Each invariant runs in CI from the phase that adds it (phases.md): I1 to I5 by the end of 2a, I6 to I8 by the end of 2b | PLAN.md section 9, D37 |
| Freshness | No lag target; the lag is stored per decision and published | D23 |
| Contract limits | 200 bodies at most 16 KiB; idle connections kept at least 5 s | D13 |
| Security | Every port bound to 127.0.0.1; no authentication on local-only services; no secrets in the repo (`.env.example` only); parameterised SQL only; refusal bodies never carry fired rules | D3, D31 |
| Reproducibility | `docker compose up` and `make bench` reproduce every README number; measured numbers come only from repo scripts | PLAN.md section 9, D29 |
| Environment | Runs on this Mac with Docker Desktop, uv, k6, git and gh; free; no cloud service, paid API, sign-up or credential. `make e2e` also needs what tally's `bootRun` needs, Java 25, already installed for project 1; nothing new is installed for it | CLAUDE.md, D26, D28 |
| Code quality | Python 3.12; ruff and mypy clean; `make audit` (pip-audit) clean; dependencies pinned in the lockfile | CLAUDE.md |
| Data | PaySim stays in git-ignored `data/` | CLAUDE.md |

## 5. Non-goals

- A distributed stream processor, a rule expression language, any UI, deep learning, online retraining (PLAN.md section 2).
- A review or release flow for held transfers; tally has no pending state (D3).
- Asking for decisions on multi-line entries or reversals (D8).
- Authentication between tally and the risk engine (D31).
- Hot reload of rules (D27).
- Currency-aware thresholds: rules compare minor units and assume CAD (D20).
- Partitioning across machines; discussed in design.md as a follow-up only.
- Any cloud, hosted or paid service (CLAUDE.md).
