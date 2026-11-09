# Project 2: Streaming risk decision engine

> Decide allow, hold or block for every ledger transfer within a latency budget, using rolling features over the ledger's event stream. Then add a model trained on public labelled data and run it in shadow next to the rules.

| | |
|---|---|
| **Wave** | 2a in wave 1 · 2b in wave 2 |
| **Dates** | Phase 0 before Nov 16 · 2a: Nov 16–29 · 2b: Feb 1–21 |
| **Built for** | Wealthsimple's Decision Platform, then Stripe, Amex, Capital One, Block and ML-systems teams |
| **Stack** | Python 3.12, uv, FastAPI, Redis, Kafka (`apache/kafka`, the same broker tally uses), Postgres, LightGBM (2b), k6, pytest + Hypothesis, pip-audit |
| **Install** | Docker Desktop, uv, k6, git, gh |
| **Depends on** | Project 1's ledger, tally (`github.com/jenishmsoni45/tally`): its event stream as merged through tally phase 4, plus a pre-commit decision call tally doesn't have yet (section 3). A stub in this repo stands in for tally in tests. |
| **Cost** | Free. Everything runs locally; PaySim needs only a free Kaggle account to download. |

**Why Python:** Wealthsimple's postings list Python. In 2b the online service and the offline training pipeline must compute features with the same code, and one language keeps them identical.

---

## 1. What you're building

Wealthsimple's Financial Risk team runs a real-time rules engine over streaming data. They measure it by fraud loss, false-positive rate and decision latency. This project is a small, honest version of that, attached to your own ledger.

What makes it stand out:
- It's the **system around a model**: feature freshness, a latency budget, a timeout policy and safe rollout. It isn't a notebook.
- Every decision is **idempotent and explainable**: the same transfer gets the same answer, with the rules that fired and the features it saw.
- 2b is graded on **public labelled data** (PaySim), not on attacks you wrote yourself.

## 2. Scope

**2a (wave 1):** a decision API the ledger calls before commit, with a hard timeout and a per-rule-set fail-open or fail-closed policy. Rolling per-account counts and sums over 1 min, 1 h and 24 h, kept in Redis and safe against duplicate and late events. Plain YAML rules, versioned. A decision log recording the fired rules and a feature snapshot. Load and fault tests with published numbers.

**2b (wave 2):** PaySim replayed as ledger transfers. One feature module shared by the online service and the offline pipeline, with parity and point-in-time tests. A LightGBM model with a time-based split and a threshold set by review capacity. Shadow mode beside the rules, plus a backtest CLI.

**Out of scope:** a distributed stream processor, a rule expression language, any UI, deep learning, online retraining, and any cloud or paid service.

## 3. Architecture (starting sketch; Phase 0 turns it into Mermaid diagrams)

```
tally (or stub): POST /transfers  (Idempotency-Key header)
  │
  ├─(1) POST /v1/decisions ──timeout──▶ Decision API ──▶ rules over features ──▶ allow | hold | block
  │     (new in tally; before its            │      ▲            (2b: model scores in shadow, logs only)
  │      posting transaction opens)          ▼      │
  │                                  decision log  Redis windows (per account: 1m / 1h / 24h)
  │                                   (Postgres)          ▲
  ├─(2) post entry + outbox row, one transaction          │
  ▼        ▼                                              │
 201     outbox relay ──▶ Kafka `ledger.entries` ──▶ feature consumer (dedupe on entry id)
```

**Contract with the ledger.** The event stream below is what tally publishes today (code merged through tally phase 4, Oct 19, 2026; tally's `docs/design.md`, "Event ordering and delivery"). Phase 0 re-checks it against tally's `main`, because tally's later phases may change it.

What tally already has:
```
POST /transfers                 header Idempotency-Key: printable ASCII, 1–255 chars, required
{ "fromAccountId": "<uuid A>", "toAccountId": "<uuid B>", "amount": 12500, "currency": "CAD",
  "description": "rent", "effectiveAt": "2026-11-20T15:04:05Z" }    description, effectiveAt optional
→ 201 the entry, the same JSON as the event value below
  same key + same body → the first 201 replayed · same key + different body → 409
  422 unbalanced, overdraft or wrong currency · 404 unknown account · 400 invalid request
Also POST /entries (up to 100 lines, summing to 0 per currency) and POST /entries/{id}/reverse.

Event: Kafka topic `ledger.entries`, 3 partitions, one message per posted entry of any kind
key   = the entry id (UUID string)
value = { "id": "<entry uuid>", "kind": "transfer", "reversesId": null, "description": "rent",
          "effectiveAt": "2026-11-20T15:04:05Z", "createdAt": "2026-11-20T15:04:05.012Z",
          "lines": [ { "id": 4101, "accountId": "<uuid A>", "amount": -12500, "currency": "CAD" },
                     { "id": 4102, "accountId": "<uuid B>", "amount":  12500, "currency": "CAD" } ] }
```
- **At-least-once.** The same entry can arrive twice. Dedupe on the entry `id`.
- **Ordered per entry, not per account.** Two entries on one account can land on different partitions and arrive in either order. On one account, line `id`s increase in commit order.
- **Signed minor units.** A negative line takes from the account, a positive one adds to it. `kind` is `transfer` (2 lines), `entry` (up to 100 lines) or `reversal` (the mirror of the entry in `reversesId`).
- **Two timestamps, no commit time.** `createdAt` is the start of tally's posting transaction. `effectiveAt` is sent by the caller (it can be backdated) and defaults to `createdAt`.
- **Successful postings only.** A refused posting (422, 404) leaves no event and no stored row.
- **Parse, don't compare bytes.** Field names are camelCase, timestamps ISO-8601 UTC. tally keeps the payload as jsonb, so key order and spacing can differ from the API response. There are no message headers; tally's internal event type (`entry.posted`) isn't sent.

What tally doesn't have yet (this project needs it; Phase 0 Q6 decides where it gets built):
```
POST /v1/decisions
{ "requestId": "<the transfer's Idempotency-Key>", "fromAccountId": "<uuid A>", "toAccountId": "<uuid B>",
  "amount": 12500, "currency": "CAD", "requestedAt": "2026-11-20T15:04:05Z" }
→ 200 { "decisionId": "d_9", "decision": "hold", "ruleSet": "v3", "fired": ["velocity_1m"] }
```
- `requestId` is tally's Idempotency-Key. The entry id doesn't exist until the posting commits, and the key is the one id a retried transfer keeps. So I1 lets a retry ask again and get the same answer.
- Proposed: tally calls this before it opens its posting transaction, so it holds no row lock or pooled connection while it waits.
- Field names are camelCase to match tally's JSON.

Keep a fixture captured from a real tally message in this repo, and test that the stub's events parse the same way, so the stub can't drift from tally.

**Rules: plain YAML, no expression language.**
```yaml
version: 3
default: allow
on_timeout: hold
rules:
  - id: velocity_1m
    action: hold
    all:
      - { feature: out_count_1m, op: ">=", value: 5 }
```

## 4. Guarantees and numbers

| # | Invariant | How it's tested |
|---|---|---|
| I1 | The same `requestId` (the transfer's Idempotency-Key) always returns the same decision. | Concurrent duplicate requests in a property test |
| I2 | Duplicate and reordered events never change feature values. | Hypothesis sequences compared against a brute-force oracle |
| I3 | Window values equal a recompute from the event log at any time `t`. | The same oracle at random query times |
| I4 | The caller never hangs: past the timeout it applies `on_timeout`. | Fault test with the service paused |
| I5 | Every decision row has a rule-set version, fired rules and a feature snapshot. | Schema constraint plus a test |
| I6 (2b) | Features for a decision at `t` use only events before `t`. | Property test that injects a future event |
| I7 (2b) | Offline replay features equal the features logged online. | Parity test on 10k sampled transfers |
| I8 (2b) | Shadow mode never changes a live decision. | Decisions identical with shadow on and off |

**Numbers to publish:**
- p50 and p99 decision latency at stated rates, and the rate where p99 crosses the budget
- Timeout rate and consumer lag
- 2b: PR-AUC, recall and fraud value caught at a fixed hold rate, false-positive rate, and the model's added latency

## 5. Questions Phase 0 must settle

Claude asks you all of these (and anything else it finds unclear) before writing any docs:

1. Features come from *committed* events, which arrive after the decision. How do we count a burst of attempts that haven't committed yet?
2. Exact windows (a sorted set per account) or bucketed counters?
3. Fail open or fail closed, per rule set? Who enforces it: the caller or the service?
4. What are the latency budget and the caller's timeout?
5. How late may an event be, and what happens to one that arrives later than that?
6. tally has no pre-commit decision call. Do we add it to tally (an extra tally phase with its own PR in that repo), or does only the stub call the decision API while the real tally is just the event source? Do the integration tests run against the real tally container, the stub, or both?
7. Which starter rules ship in 2a, and what should each one catch?
8. What does `hold` mean when tally has no pending state? Options: refuse the transfer with its own problem type (and which status), or build tally's holds stretch (authorise, capture, void).
9. Which events feed the features: only `kind: transfer`, or multi-line entries and reversals too? Does a reversal take back what its original entry added to the windows?
10. Which time drives the windows: `createdAt` (tally's clock, the start of the posting transaction) or `effectiveAt` (the caller's, can be backdated)? This sets how "late" is measured in Q5 and what "before t" means in I6.
11. The event doesn't carry the Idempotency-Key, so a decision (keyed by `requestId`) can't be joined to the entry it allowed. Should tally add the key or the decision id to the event, or do we skip the join?
12. (2b) Which PaySim sample (days, rows, accounts)? Before choosing features, check how often accounts repeat. If most senders appear once, receiver-side features will carry more of the signal.
13. (2b) What review capacity (share of transfers held) sets the model's threshold?
14. (2b) Which features may the model use? PaySim's balance columns make the problem artificially easy, so the default is no.

## 6. Phases

Each phase is one PR. Phase 0 writes the full acceptance criteria into `docs/phases.md`; these are the seeds.

| Phase | Target dates | Scope | Done when | Look closely at |
|---|---|---|---|---|
| **0** Plan | Before Nov 16 | Questions answered; requirements, architecture (Mermaid), design, phases, decisions | No open questions; docs consistent | Contract (re-checked against tally's `main`), invariants, the answers to Q1–Q11 |
| **1** Scaffold | Nov 16 | Layout, Compose (Redis, Kafka, Postgres, service, ledger stub), Makefile, CI, stub endpoints | `make up` healthy; CI green | Compose health checks, CI steps |
| **2** Decisions core | Nov 17–19 | Decision API, decision log, idempotency, ledger stub with timeout and on_timeout | I1, I4, I5 pass; stub round trip works | The idempotency path; the timeout handling |
| **3** Rolling features | Nov 20–23 | Consumer, Redis windows, dedupe, late events | I2, I3 pass against the oracle | Window update logic; offset commits |
| **4** Rules | Nov 24–25 | YAML loader, versioning, fired rules, feature snapshot | Rule fixtures produce the expected decisions | Rule evaluation order; version handling |
| **5** Prove 2a | Nov 26–29 | k6 bench, fault and adversarial tests, README, postmortem, talk outline | `make bench` reproduces the README; definition of done met | Whether the numbers are honest |
| **6** Replay (2b) | Feb 1–7 | PaySim loader → transfers; offline features from the shared module | I6, I7 pass | The point-in-time join |
| **7** Model (2b) | Feb 8–14 | Training pipeline, time split, threshold, model card | Model card reproducible from one command | Leakage; the split; the threshold logic |
| **8** Shadow and ship 2b | Feb 15–21 | In-process scorer in shadow, backtest CLI, latency impact, README | I8 passes; backtest report; p99 still in budget | Shadow isolation; the claims in the report |

## 7. Repo layout (Phase 1 creates it)

```
project-2-risk-engine/
├── CLAUDE.md  PLAN.md  README.md  .github/pull_request_template.md
├── docs/          requirements.md  architecture.md  design.md  decisions.md  phases.md  postmortem.md
├── risk/          api.py  decisions.py  store.py  features/  rules/
├── ledger_stub/   minimal tally stand-in: calls the decision API, publishes tally-format events to `ledger.entries`
├── ml/            replay.py  train.py  shadow.py  backtest.py   (2b)
├── tests/         invariants/  adversarial/  unit/  integration/
├── bench/         k6 scripts, results/
├── data/          PaySim CSV (git-ignored)
├── docker-compose.yml  Makefile  pyproject.toml  .github/workflows/ci.yml
```

## 8. Prompts

Run `claude` inside this folder. The workflow rules live in `CLAUDE.md`, so the prompts stay short.

### P0 · Phase 0: plan (paste once)

```text
Start phase 0 (planning). Follow the Phase 0 rules in @CLAUDE.md: no code and no scaffolding.
1. Read @PLAN.md. List every open question: start with section 5, then add anything ambiguous, missing or contradictory. Ask me in batches of up to 5, each with options, trade-offs and your recommendation. Wait for my answers before the next batch.
2. Record every question and answer in docs/decisions.md.
3. When nothing is open, write:
   - docs/requirements.md: users, user stories with acceptance criteria, non-functional requirements with numbers, non-goals
   - docs/architecture.md: Mermaid diagrams for the components, the decision sequence (including timeout), the event-to-feature flow, and the data model
   - docs/design.md: contracts, data model, invariants I1–I8, failure modes, alternatives rejected, prior art, test plan, measurement plan
   - docs/phases.md: phases 1–8 from PLAN.md section 6, each with scope, testable acceptance criteria, tests to add and out of scope
4. Review the docs as a skeptical staff engineer: gaps, contradictions, untestable criteria, anything needing a paid or cloud service or an install on this Mac beyond the Install row. Fix them, and ask me if a fix needs a decision.
5. Create branch phase-0-plan, commit, push, and open a docs-only PR with the template. Then stop.
```

### P1 · Start a phase (every phase after 0)

```text
Start phase <N>. Follow the phase workflow in @CLAUDE.md: sync main and confirm earlier phase PRs are merged, branch, read the phase in @docs/phases.md, ask me before coding if anything is unclear, build the whole phase with tests, run lint and tests, self-review the full diff and fix what you find, then push and open the PR with the template. Stop when the PR is open.
```

### P2 · Address my review

```text
I've reviewed PR #<n>. Read every comment (`gh pr view <n> --comments` and the inline comments via `gh api repos/{owner}/{repo}/pulls/<n>/comments`). Fix each one on the same branch, or explain why you disagree. Run lint and tests, push, and reply with a short list of comment → what changed. Don't merge.
```

### P3 · Explain a PR to me (while reviewing)

```text
Walk me through PR #<n> as if onboarding me to code I must defend in an interview. Follow the review-guide order. For each file: what it does, why it's built this way, the alternative rejected, and how the tests prove it. Then ask me 5 questions about it, one at a time, and correct what I get wrong. Don't change any code.
```

### P4 · Mock interview (after phase 5, and again after phase 8)

```text
Act as an engineer interviewing me at Wealthsimple (risk platform) or Stripe (fraud). Interview me about this project for 30 minutes, one question at a time. Start with "walk me through it", then go deep wherever my answer is vaguest: failure modes, why X over Y, what changes at 100x traffic. Use @PLAN.md section 9 and @docs/decisions.md as a starting point and follow up on my answers. Don't give me answers. At the end, score clarity, depth and ownership out of 5, and list 3 topics to study.
```

## 9. Finish line

**Definition of done** (2a after phase 5; 2b after phase 8)
- [x] Phase 0 docs merged and kept current
- [x] I1–I5 (and I6–I8 for 2b) run in CI
- [x] `docker compose up` + `make bench` reproduce every README number
- [x] `docs/postmortem.md` on one real bug found during the build
- [ ] 3-minute demo video (you record it) + 20-minute talk outline (the outline is `docs/talk.md`; the video is still to record)
- [x] README section on how you verified it (D62 dropped the part on how it was built)

**Resume line** (fill in from your own measurements):
> Built a streaming risk engine that scores ledger transfers in-line within an X ms p99 budget using sliding-window features, with idempotent decisions and a tested fail-closed path. Replayed N labelled PaySim transfers to train a model run in shadow beside the rules; at a fixed review capacity it caught Y% of fraud value versus Z% for rules alone.

**Questions to be ready for**
- What happens when the feature store lags behind the stream?
- Fail open or fail closed, and for which rules?
- How do you avoid double-counting a duplicate event in a window?
- How do you know your offline features match what the service saw?
- How do you know a new rule or model is better, not just stricter?
- How would you partition this across machines?
