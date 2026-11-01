# Architecture

How the risk engine's parts fit together. [requirements.md](requirements.md) says what it must do, [design.md](design.md) gives the contracts and the detailed behaviour, and [decisions.md](decisions.md) records why (D-numbers).

## 1. Components (2a)

```mermaid
flowchart LR
    customer(["Customer or k6"])
    subgraph callers["Callers"]
        stub["ledger stub<br/>POST /transfers, outbox relay"]
        tally["tally (make e2e only)<br/>POST /transfers, outbox relay"]
    end
    subgraph engine["risk engine: Docker Compose, ports on 127.0.0.1"]
        api["decision API<br/>POST /v1/decisions"]
        consumer["feature consumer"]
        redis[("Redis<br/>windows, attempts, watermark")]
        pg[("Postgres<br/>decisions, bad_events, stub schema")]
        kafka[("Kafka<br/>ledger.entries, 3 partitions")]
        migrate["migrate (one-shot)"]
        topics["topics (one-shot)"]
    end
    tallykafka[("tally's Kafka<br/>host.docker.internal:9094")]

    customer -->|"transfer + Idempotency-Key"| stub
    customer -.->|"make e2e"| tally
    stub -->|"decision request, 100 ms timeout"| api
    tally -.->|"decision request, 100 ms timeout"| api
    api -->|"read features, record attempt"| redis
    api -->|"decision row"| pg
    stub -->|"keys, saved responses, outbox"| pg
    stub -->|"relay publishes events"| kafka
    tally -.->|"relay publishes events"| tallykafka
    kafka --> consumer
    tallykafka -.->|"make e2e"| consumer
    consumer -->|"one sorted-set member per account and entry"| redis
    consumer -->|"malformed messages"| pg
    migrate -->|"schema"| pg
    topics -->|"creates ledger.entries"| kafka
```

| Component | Role | Code (PLAN.md section 7) |
|---|---|---|
| Decision API | Validates the request, replays a known `requestId`, reads features, evaluates rules, stores the decision, records the attempt, answers. In 2b it also scores the shadow model. | `risk/api.py`, `risk/decisions.py`, `risk/store.py` |
| Feature consumer | Reads `ledger.entries`, nets each entry per account, writes window members, keeps the watermark, stores malformed messages | `risk/features/` |
| Feature module | The only feature logic: window membership, netting, the 31 features. Used by the API, the consumer and (2b) the offline pipeline | `risk/features/` |
| Rules | Loads and validates the YAML rule set at startup and evaluates it | `risk/rules/` |
| Ledger stub | tally's transfer path for CI: Idempotency-Key rules, the decision call with tally's timeout and backup answer, an outbox and relay that publish tally-format events (D24) | `ledger_stub/` |
| migrate | Applies numbered SQL migrations before the other services start (D40w) | `risk/migrations/` |
| topics | Creates `ledger.entries` with 3 partitions before the consumer and the stub start (D42) | `docker-compose.yml` |
| Redis | Window and attempt sorted sets, the watermark. Rebuildable, never the source of truth (D40r) | |
| Postgres | The decision log, `bad_events`, the stub's schema | |
| Kafka | This repo's own broker, `apache/kafka:4.3.1` like tally's; `ledger.entries` with 3 partitions like tally's | |

The decision API, the consumer and the stub are separate Compose services built from one image, so a consumer crash never stops decisions (D40j).

## 2. Decision sequence, including the timeout

The caller is tally or the stub; both behave the same (D24). Deadlines: the caller gives up after 100 ms (D4); the decision API gives up on its own parts after 60 ms (D40d).

```mermaid
sequenceDiagram
    autonumber
    participant U as Customer
    participant C as Caller (tally or stub)
    participant A as Decision API
    participant R as Redis
    participant P as Postgres

    U->>C: POST /transfers (Idempotency-Key)
    C->>C: validate (400), replay a saved response for this key and stop
    C->>A: POST /v1/decisions with requestId, from, to, amount, currency, requestedAt
    Note over C,A: caller deadline 100 ms, API deadline 60 ms
    A->>P: find decision by requestId
    alt requestId already decided
        alt same from, to, amount, currency
            A-->>C: 200 stored decision, current onTimeout
        else different transfer
            A-->>C: 409 problem
        end
    else new requestId
        A->>R: one script reads 5 sorted sets and the watermark
        A->>A: 31 features, lag = t − watermark, evaluate rules, strictest wins
        A->>P: insert decision row (on conflict, answer as a replay)
        A->>R: record the attempt (requestId at requestedAt)
        A-->>C: 200 with decisionId, decision, ruleSet, fired, onTimeout
    end
    Note over A: Redis or Postgres slow or down past 60 ms gives 503, no row

    alt 200 allow
        C->>C: post, riskCheck decided, publish later via outbox
        C-->>U: 201
    else 200 hold or block
        C-->>U: 403 transfer-held or transfer-blocked, with decisionId
    else 409
        C-->>U: 409 key used for a different request
    else no valid 200 within 100 ms (timeout, 503, refused, bad body)
        C->>C: backup answer = latest onTimeout seen (default hold)
        alt backup allow
            C->>C: post, riskCheck fallback, decisionId null
            C-->>U: 201
        else backup hold or block
            C-->>U: 503 risk-unavailable (retry with the same key)
        end
    end
```

Points the diagram makes:
- The caller holds no transaction or database connection while it waits (tally-integration.md 4.1).
- A refused transfer saves no key at the caller, so a retry asks again and gets the same decision (I1); a key refused by the backup answer gets a real decision once the API is back.
- Only complete evaluations are stored; a 503 leaves nothing behind (D22).
- Concurrent first requests with one `requestId` race on the insert; the loser reads the winner's row and answers it as a replay (or 409 if the transfer differs), so both get one decision (I1).

## 3. From event to feature

```mermaid
flowchart TD
    msg["Kafka message<br/>key: entry id, value: entry JSON"] --> parse{"parses and<br/>passes validation?"}
    parse -- no --> bad["store in bad_events, count"]
    parse -- yes --> kind{"kind"}
    kind -- reversal --> skip["ignored (D17)"]
    kind -- "transfer or entry" --> net["net the lines per account"]
    net --> late{"createdAt older than<br/>watermark − 24 h − 5 min?"}
    late -- yes --> drop["drop, count as late"]
    late -- no --> add["per account with a non-zero net:<br/>add member amount:entryId to its out or in set,<br/>score createdAt; a repeat is a no-op"]
    add --> wm["watermark = max(watermark, createdAt)"]
    wm --> trim["remove members older than<br/>watermark − 24 h − 5 min"]
    bad --> commit["commit offsets once the writes are done"]
    skip --> commit
    drop --> commit
    trim --> commit
```

```mermaid
flowchart LR
    req["decision at t,<br/>sender A, receiver B"] --> read["one Redis script reads<br/>A out, A in, B in, B out, A attempts"]
    read --> win["per window W in 1 min, 1 h, 24 h:<br/>count and sum of members with t − W ≤ score < t"]
    win --> feats["31 features (D19)<br/>+ lag = t − watermark"]
    feats --> rules["rules: strictest fired action, else default"]
    feats --> log["decision row: snapshot of all 31"]
```

Why this order is safe:
- Every write is idempotent: the same entry always yields the same member, so a duplicate changes nothing (I2), and arrival order doesn't matter (I2). Offsets are committed only after the writes, so a crash replays messages rather than losing them (D40h).
- Late events are applied whenever they can still count (D15). The watermark is event time, so a replay behaves the same at any speed.
- The read takes a window ending just before t, so an event at t or later never counts (I6).

## 4. Data model

### Postgres

```mermaid
erDiagram
    DECISIONS {
        uuid decision_id PK
        text request_id UK
        uuid from_account_id
        uuid to_account_id
        bigint amount
        text currency
        timestamptz requested_at
        timestamptz decided_at
        text decision
        int rule_set_version
        text rule_set_hash
        jsonb fired
        jsonb features
        bigint feature_lag_ms
        float shadow_score "2b, nullable"
        text shadow_model "2b, nullable"
    }
    BAD_EVENTS {
        bigint id PK
        text topic
        int kafka_partition
        bigint kafka_offset
        bytea message_key
        bytea message_value
        text error
        timestamptz received_at
    }
    STUB_TRANSFERS {
        text idempotency_key PK
        text request_hash
        int response_status
        jsonb response_body
        uuid entry_id
        uuid decision_id
        text risk_check
        timestamptz created_at
    }
    STUB_OUTBOX {
        bigint id PK
        uuid entry_id
        jsonb payload
        timestamptz created_at
        timestamptz published_at
    }
    SCHEMA_MIGRATIONS {
        int version PK
        timestamptz applied_at
    }
    STUB_TRANSFERS ||--o| DECISIONS : "decision_id"
    STUB_TRANSFERS ||--o| STUB_OUTBOX : "entry_id"
```

- `decisions` is the decision log and the attempt log (D18). Its constraints enforce I5; design.md lists them.
- `stub_transfers` and `stub_outbox` live in the `stub` schema (D40l). Only posted transfers have a saved response and an outbox row, like tally.

### Redis

| Key | Type | Member | Score | Written by |
|---|---|---|---|---|
| `f:{accountId}:out` | sorted set | `{amount}:{entryId}` | `createdAt`, epoch ms | consumer |
| `f:{accountId}:in` | sorted set | `{amount}:{entryId}` | `createdAt`, epoch ms | consumer |
| `a:{accountId}` | sorted set | `{amount}:{requestId}` | `requestedAt`, epoch ms | decision API |
| `wm` | string | latest `createdAt` applied, epoch ms | | consumer |

The amount comes first in the member because a `requestId` may contain a colon. A decision reads the sender's `out`, `in` and `a` sets and the receiver's `in` and `out` sets.

## 5. Deployment

| Service | Image | Host port (127.0.0.1) | Depends on |
|---|---|---|---|
| `migrate` | this repo's image | none | postgres healthy |
| `topics` | `apache/kafka:4.3.1` | none | kafka healthy |
| `api` | this repo's image | 8000 | migrate done, redis and postgres healthy |
| `consumer` | this repo's image | 8002 (metrics only, D40z) | migrate and topics done, redis and kafka healthy |
| `stub` | this repo's image | 8001 | migrate and topics done, api and kafka healthy |
| `postgres` | `postgres:18.6` | 5433 | |
| `redis` | `redis:8.10.3` (D42) | 6379 | |
| `kafka` | `apache/kafka:4.3.1` | 19092 | |

The ports avoid tally's (D31), so both stacks run at once for `make e2e`.

- **Stub mode** (default, CI): the stub is the caller and this repo's Kafka carries the events.
- **e2e mode** (`make e2e`, local only, D26): tally from the owner's clone is the caller, with `TALLY_RISK_ENABLED=true` and `TALLY_RISK_URL=http://localhost:8000`; the consumer reads tally's broker at `host.docker.internal:9094`. `make e2e` starts this repo's stack fresh and gives the consumer a new consumer group that starts at the end of tally's topic, so events left from tally's own runs never reach the windows.

## 6. 2b: replay, offline pipeline, model and shadow

```mermaid
flowchart LR
    csv[("data/ PaySim CSV<br/>git-ignored")] --> profile["profile script (D32)"]
    csv --> loader["loader: TRANSFER and CASH_OUT,<br/>UUIDs, cents, times in the hour"]
    loader --> offline["offline pipeline<br/>shared feature module"]
    loader --> replay["replay CLI<br/>waits for each event (D36)"]
    replay --> stubr["ledger stub, replay mode<br/>record-only, replay clock"]
    stubr --> apis["decision API<br/>+ shadow scorer (D39)"]
    stubr --> kafka2[("Kafka")]
    kafka2 --> consumer2["consumer"]
    consumer2 --> redis2[("Redis")]
    apis --> log[("decisions: features, decision, shadow score")]
    offline --> train["train: LightGBM, split by days (D38),<br/>threshold at 1% held (D34)"]
    train --> card["model file + model card"]
    card --> apis
    offline --> backtest["backtest CLI: rules vs model (D40q)"]
    log --> parity{"I7: logged online =<br/>offline recompute"}
    offline --> parity
```

- The offline pipeline and the consumer share one feature module; the offline side feeds it the loader's transfers as ledger events, in time order.
- In replay mode the stub posts every transfer whatever the decision, and takes each transfer's time from the replay (D40u, D40v), so online and offline features cover the same events.
- The shadow scorer runs inside the decision API after the rules decide; its output is stored and never read by the decision (I8).
