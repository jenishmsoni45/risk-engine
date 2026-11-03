# Talk outline: deciding before the money moves

A 20-minute talk on 2a: a risk decision made inside a ledger's transfer path, within a latency budget, from features built off the ledger's own event stream. Every number shown comes from README.md's tables, which `make report` writes from `bench/results/`. None is quoted here, so the outline can't drift from them.

## 1. The problem (2 min)

- A ledger (tally) is about to post a transfer. Should it go through, wait for review, or be refused?
- The answer has to come before the posting transaction opens, inside the caller's 100 ms timeout (D4), and an outage of the risk engine must never stall payments.
- What makes it hard: features are rolling counts and sums over 1 min, 1 h and 24 h, built from a stream that repeats, reorders and arrives late.

## 2. The shape of the system (3 min)

- Diagram: architecture.md section 1. The caller, the decision API, the feature consumer, Redis, Postgres, Kafka.
- Two paths that never wait for each other: the decision path reads features; the event path writes them. A consumer crash never stops decisions (D40j).
- The ledger stub: tally's transfer path in Python, so CI tests the real contract without the JVM (D24), and `make e2e` checks it against tally itself.

## 3. Never hang: the timeout and the backup answer (3 min)

- Sequence diagram: architecture.md section 2.
- The caller gives up at 100 ms and applies the latest `onTimeout` it saw, `hold` by default (D10). The API gives up on its own parts at 60 ms, so it answers 503 rather than a late 200 (D40d).
- Fail open or fail closed, and per what? The rule set chooses, and every 200 carries its choice, so changing it needs no deploy on the caller.
- Demo: pause the API; tally answers 503 `risk-unavailable`; unpause; the retry with the same key gets a real decision.

## 4. Same key, same answer (2 min)

- I1: a retried Idempotency-Key never turns a hold into an allow.
- Concurrent first requests race on a unique insert; the loser reads the winner's row and answers it as a replay.
- `tests/adversarial/test_floods.py`: a burst of concurrent requests with one `requestId` leaves one row.

## 5. Windows that survive a messy stream (3 min)

- One sorted-set member per account and entry, `amount:entryId`, scored by `createdAt`. A repeat is a no-op, order doesn't matter (I2).
- The watermark and the lateness margin: what "too late" means and why it is counted rather than hidden (D15).
- How it is proved: a Hypothesis oracle recomputes every window from the raw event log (I3), and the floods of repeated and late events leave features exactly as the oracle says.
- The question to be ready for: what happens when the feature store lags? Decisions keep going, every row stores how stale its features were (`feature_lag_ms`), and the consumer's lag is a published number (D23).

## 6. Explaining every decision (1 min)

- I5: each row stores the rule-set version, the fired rules and a snapshot of all 31 features.
- An analyst can answer "why was this held?" months later without recomputing anything.

## 7. The numbers (3 min)

- Show README.md's tables: p50 and p99 per rate, straight to the API and through the stub; the first rate whose p99 passes 50 ms; the timeout rate; the consumer lag.
- The hot account: p99 against the size of one account's 24 h window, and why the read cost grows with it (D16).
- A bench that measured itself: the first hot-account run used a hot sender, and every request added an attempt to the window it was measuring. Making the hot account a receiver kept the window at its preloaded size (D50).
- How the numbers are kept honest: constant arrival rate, client-side latency, a fresh stack per scenario, the conditions file beside every result, and `make check-readme` failing CI when README's tables differ from the results.

## 8. What broke (2 min)

- The bug in `docs/postmortem.md`: Kafka's log reset while Postgres remembered, and an idempotent insert turned the mismatch into silence.
- The lesson: state keyed by another store's positions has to share that store's lifetime, and tests that only start from empty can't find lifetime bugs.

## 9. Beyond one machine (1 min)

- design.md section 13: a stateless API behind a load balancer, the consumer scaled by partitions, Redis sharded by account with a hash tag, and the hot account still the limit on its shard.

## Questions to be ready for (PLAN.md section 9)

- What happens when the feature store lags behind the stream?
- Fail open or fail closed, and for which rules?
- How do you avoid double-counting a duplicate event in a window?
- How would you partition this across machines?
- (2b) How do you know your offline features match what the service saw, and that a new rule or model is better, not just stricter?
