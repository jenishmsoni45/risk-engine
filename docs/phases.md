# Phases

Phases 1 to 8 from PLAN.md section 6. Each phase is one PR, started only when the owner names it, and follows the workflow in CLAUDE.md. Every acceptance criterion names the test or check that proves it, and the PR ticks each one. `make lint`, `make test` and `make audit` pass at the end of every phase. A test added in one phase keeps passing in every later one.

## Phase 1 · Scaffold (Nov 16)

**Scope.** The layout of PLAN.md section 7; `pyproject.toml` and `uv.lock` for Python 3.12; ruff and mypy settings; one Dockerfile for the decision API, consumer, stub and `migrate`; `docker-compose.yml` with Redis, Postgres, Kafka, `topics`, `migrate`, `api`, `consumer` and `stub` (D31, D40j, D42); health checks; the migration runner (D40w); `GET /health` on the API and the stub, `GET /metrics` on all three (D40t); the `ledger.entries` topic, created by the one-shot `topics` service (D42); `.env.example`; a Makefile with `up`, `down`, `test`, `lint`, `audit`, `clean` and `purge` (D41); the install safeguards in `pyproject.toml` (D41); the CI workflow.

**Acceptance criteria**
1. The phase starts by running `uv --version`; if it fails, work stops and the owner is asked (D28).
2. `make up` brings every long-running service to healthy and `migrate` and `topics` to a clean exit. (`tests/integration/test_stack.py::test_services_healthy`)
3. `make down` leaves no container of this project, running or stopped. (a CI step after the tests: `make down`, then `docker compose ps -aq` prints nothing)
4. Every published port is bound to 127.0.0.1 and equals D31's table plus the consumer's metrics port (D40z). (`tests/integration/test_stack.py::test_ports`, from `docker compose config`)
5. Every image has an exact tag, never `latest`; every Python dependency is pinned in `uv.lock`. (`tests/integration/test_stack.py::test_images_pinned`; `uv lock --check` in `make lint`)
6. After `make up`, `ledger.entries` exists with 3 partitions. (`tests/integration/test_stack.py::test_topic`)
7. `GET /health` answers 200 on ports 8000 and 8001, and each service's `/metrics` answers Prometheus text. (`tests/integration/test_stack.py::test_health_and_metrics`)
8. Running `migrate` twice applies each migration once. (`tests/integration/test_migrate.py`)
9. `.env` and `data/` are git-ignored, and `.env.example` holds only dummy values. (`tests/unit/test_repo_hygiene.py`)
10. `make lint` (ruff check, ruff format check, mypy), `make test` and `make audit` pass locally, and CI runs the same three with the Compose stack and is green on the PR.
11. D41's safeguards stay on: `pyproject.toml` sets `exclude-newer` to a fixed UTC time, `no-build = true`, `python-downloads = "never"` and `package = false`; every direct dependency is on D40s's list; every locked package comes from PyPI and has a wheel; and every install in the Makefile, the Dockerfile and CI uses `uv sync --locked`. (`tests/unit/test_dependency_safety.py`)
12. `make clean` leaves no container, volume or network of this project, no image built from this repo and no `.venv`, and leaves `data/`, `.env` and every tracked file as they were. (a CI step after the tests: a file is put in `data/` and a dummy `.env` is written, then `make clean` runs, then the checks)
13. `make purge` also removes every image this repo pins that no container uses, and empties uv's cache. (a CI step after criterion 12's)

**Tests to add.** `tests/integration/test_stack.py`, `tests/integration/test_migrate.py`, `tests/unit/test_repo_hygiene.py`, `tests/unit/test_dependency_safety.py`.

**Out of scope.** Any decision, feature or rule logic; the stub's `POST /transfers`.

## Phase 2 · Decisions core (Nov 17–19)

**Scope.** The `decisions` migration with I5's constraints (design.md section 2; the 31-name check comes in Phase 3); `POST /v1/decisions` (design.md 1.1 and section 4) with a rule set holding only `version`, `default` and `on_timeout` and no rules yet; idempotency and the 409; the current `onTimeout` on replays; the 60 ms deadline; keep-alive; the stub's `POST /transfers`, `stub` schema, outbox and relay (design.md 1.3, without replay mode); the four fixtures from real tally and the contract test (D25).

**Acceptance criteria**
1. A valid request gets a 200 with the five fields in the formats of design.md 1.1. (`tests/integration/test_decision_api.py::test_response_shape`)
2. Each invalid input in design.md 1.1 gets a 400 and leaves no row. (`tests/unit/test_validation.py`, `tests/integration/test_decision_api.py::test_bad_requests`)
3. I1: concurrent duplicates of generated requests give one row and identical decisions per `requestId`. (`tests/invariants/test_i1_same_request.py`)
4. The same `requestId` with a changed sender, receiver, amount or currency gets 409 and changes nothing; a changed `requestedAt` alone replays. (`tests/invariants/test_i1_same_request.py::test_conflict`)
5. After a restart with a different `on_timeout`, a replay returns the stored decision with the new `onTimeout`. (`tests/integration/test_decision_api.py::test_replay_current_on_timeout`)
6. With Redis paused, and separately with Postgres paused, the API answers 503 within 60 ms plus the D40m margin and stores no row. (`tests/adversarial/test_api_dependencies.py`)
7. A connection left idle for 5 s is reused for the next request. (`tests/integration/test_decision_api.py::test_keep_alive`)
8. I5, schema half: an insert missing the version, the hash, `fired` or `features` is refused; a real decision's row holds all of them and the lag. (`tests/invariants/test_i5_decision_row.py`)
9. Stub, against the real API: an allowed transfer answers 201, and its event reaches `ledger.entries` with `decisionId` and `riskCheck: decided`. (`tests/integration/test_stub.py::test_allow_round_trip`)
10. Stub, against a fake decision API: hold and block give their 403s with `decisionId` and no event; a 409 gives 409; the same key and body replay without calling the fake; a different body gives 409; a refused transfer saves no key. (`tests/integration/test_stub.py`)
11. I4: against the API paused, a fake that never answers, one that answers late, one that sends headers and no body, a 500, a body over 16 KiB, invalid JSON and an invalid decision value, the stub answers within 100 ms plus the margin with the backup answer: 503 `risk-unavailable` under the default `hold`, and `fallback` after a reply whose `onTimeout` was `allow`. (`tests/invariants/test_i4_caller_never_hangs.py`)
12. The four fixtures captured from real tally (an allowed transfer, a backup-allowed transfer, an entry, a reversal) and the stub's events parse to the same model, with the same field names, JSON types and timestamp format. (`tests/integration/test_contract_tally.py`; capture steps in `tests/fixtures/tally/README.md`)

**Tests to add.** As named above, plus a fake decision API helper for the stub tests.

**Docs.** `tests/fixtures/tally/README.md` records how and when the fixtures were captured.

**Out of scope.** Window features, attempts, rules beyond `default`.

## Phase 3 · Rolling features (Nov 20–23)

**Scope.** The shared feature module (design.md 3.5); the consumer (design.md 3.2) with netting, members, watermark, trimming, late drops, `bad_events` and offset commits; attempts (3.3); the feature read and the 31-feature snapshot with lag (3.4); a migration adding the 31-name check; the brute-force oracle in the tests.

**Acceptance criteria**
1. I2: generated entries delivered with repeats and in shuffled orders give features equal to the oracle's over the distinct entries. (`tests/invariants/test_i2_duplicates_order.py`)
2. I3: at random query times inside the retained range, late events included, every window equals the oracle's recompute. (`tests/invariants/test_i3_recompute.py`)
3. Netting: −100 and +30 on one account is one outflow of 70; a zero net adds nothing; a reversal adds nothing. (`tests/unit/test_netting.py`)
4. Window edges: an event at `t − W` counts and an event at `t` doesn't. (`tests/unit/test_windows.py`)
5. An event older than watermark − 24 h − 5 min is dropped and counted; one just inside is applied. (`tests/adversarial/test_late_events.py`)
6. Each malformed class in design.md 1.2 is stored once in `bad_events` and counted, and a valid event after it on the same partition is applied. (`tests/adversarial/test_bad_events.py`)
7. The consumer killed after writing and before committing restarts with features still equal to the oracle's. (`tests/adversarial/test_consumer_crash.py`)
8. Every new decision adds one attempt; a replayed `requestId` adds none; attempt features equal a recompute from `decisions`. (`tests/integration/test_attempts.py`)
9. Every decision row holds all 31 features and the lag, the migration refuses a snapshot missing a name, and the logged features equal the oracle's at that `t`. (`tests/integration/test_feature_snapshot.py`)

**Tests to add.** As named above, plus the oracle in `tests/oracle.py`.

**Out of scope.** Rules, rebuilding Redis (Phase 5).

## Phase 4 · Rules (Nov 24–25)

**Scope.** The full rule-file loader and validation (design.md 1.4), evaluation (section 5), the starter rule set (D20), the fired list, the version and hash.

**Acceptance criteria**
1. Each invalid file stops the service at startup with a message naming the problem: not YAML, a missing or unknown key, a repeated or malformed id, an unknown feature, a bad operator, a non-integer value, an `allow` action, an empty `all`, more than 100 rules. (`tests/unit/test_rules_loader.py`; one container-level check in `tests/integration/test_rules_startup.py`)
2. The strictest fired action wins, every fired rule is listed in file order, and no fired rule gives `default`. (`tests/unit/test_rules_eval.py`, table-driven)
3. For each starter rule, features one below the threshold don't fire it and features at the threshold do; a hold and a block firing together give `block` with both listed. (`tests/unit/test_starter_rules.py`)
4. Each starter rule fires end to end at least once, from events published to `ledger.entries` and requests to the API. (`tests/integration/test_starter_rules.py`)
5. Held and blocked decisions add attempts too. (`tests/integration/test_attempts.py::test_refused_attempts_count`)
6. Rows store version 1 and the file's SHA-256; after a restart with a changed file, new decisions carry the new version, and replays keep their stored `ruleSet` with the new `onTimeout`. (`tests/integration/test_rule_versions.py`)
7. With 100 rules whose 64-character ids all fire, the 200 body is under 16 KiB. (`tests/integration/test_body_size.py`)

**Out of scope.** Tuning thresholds (2b measures them).

## Phase 5 · Prove 2a (Nov 26–29)

**Scope.** k6 scenarios and `make bench` (design.md section 12); the results report script; fault and adversarial tests (design.md section 7); `make rebuild-features`; `make e2e` against real tally (D26); README.md; `docs/postmortem.md`; a 20-minute talk outline.

**Acceptance criteria**
1. `make bench` runs the direct, through-the-stub and hot-account scenarios and writes each k6 summary and conditions file to `bench/results/<UTC date>/`. (run by hand; files committed)
2. The report script turns those files into p50 and p99 per stated rate, the first rate whose p99 passes 50 ms, the timeout rate and the consumer lag, and `make check-readme` fails if README.md's tables differ from its output. (`bench/report.py`, `make check-readme`)
3. Fault tests show each failure-table row of design.md section 7 for the API, Redis, Postgres and Kafka paused and the consumer killed. (`tests/adversarial/test_faults.py`)
4. After Redis is flushed, `make rebuild-features` restores features equal to the oracle's. (`tests/adversarial/test_rebuild.py`)
5. A flood of every event repeated, a flood of late events, and a burst of concurrent requests with one `requestId` (its size stated in the test) leave features and rows exactly as the oracle and I1 say. (`tests/adversarial/test_floods.py`)
6. `make e2e` passes against real tally, on a fresh stack whose consumer group starts at the end of tally's topic: every decided entry's `decisionId` is in `decisions`, a held transfer gets tally's 403, and with the API stopped tally answers 503 `risk-unavailable`. (local only; output pasted in the PR)
7. I1 to I5 run in CI.
8. README.md (pitch, architecture, how to run, the numbers from the report script, how it was built and verified), `docs/postmortem.md` on one real bug found during the build, and the talk outline exist.

**Out of scope.** Optimising throughput beyond measuring it; partitioning across machines (a talk topic only).

## Phase 6 · Replay (Feb 1–7)

**Scope.** The PaySim profile script (D32), loader (D33, D40p), the stub's replay mode (D40u, D40v), the replay CLI with its wait (D36), the offline pipeline on the shared module, the generated PaySim-format data for CI (D37), `make parity`, the PaySim rule set (D54). The UTC instant that PaySim's first step maps to is chosen here and recorded in decisions.md, since the hour of day (D35) depends on it.

**Stop and ask.** After the profile runs, the owner confirms the sample (D32). The starter thresholds are CAD cents (D20) and PaySim's amounts are in an unnamed currency, so the profile also reports what share of transfers the starter rules would hold; if that share makes the rules-versus-model comparison meaningless, the owner decides whether to add a PaySim rule set version before Phase 7.

**Acceptance criteria**
1. The profile's report is in `ml/reports/` and the owner's confirmation is in decisions.md.
2. Two loader runs on one CSV give identical output; UUID v5 mapping, cents rounded half to even, zero amounts skipped and counted, and times spread evenly in file order within each hour are unit-tested. (`tests/unit/test_paysim_loader.py`)
3. Replay mode is active only with `STUB_REPLAY=1`: every transfer is posted with `riskCheck: decided`, a transfer without a decision gets 503 and stops the replay, and the normal-mode tests are unchanged. (`tests/integration/test_stub_replay.py`)
4. With a relay delay above 0, the replay doesn't send the next request until the watermark has reached the previous transfer's time. (`tests/integration/test_replay_wait.py`)
5. I6: events at or after `t` change no feature, online or offline. (`tests/invariants/test_i6_point_in_time.py`)
6. I7 in CI: on generated data, every logged feature snapshot equals the offline recompute exactly. (`tests/invariants/test_i7_parity.py`)
7. `make parity` on about 10k real transfers gives exact equality; its output is recorded in the PR.
8. The offline pipeline and the decision API compute features with the same function from `risk/features/`. (`tests/unit/test_one_feature_module.py`, an identity check on the imported function)
9. Nothing under `data/` is tracked. (`tests/unit/test_repo_hygiene.py`)
10. `make paysim-rules` writes `ml/rules/paysim.yaml` and its report in `ml/reports/`; the rule loader accepts the file; the scaling and the factor search are unit-tested on generated data. (`tests/unit/test_paysim_rules.py`)

**Out of scope.** Training or loading a model.

## Phase 7 · Model (Feb 8–14)

**Scope.** The confirmation of D30; the training pipeline; the split (D38), inputs (D35) and threshold (D34); the model card; `make model`.

**Acceptance criteria**
1. Before any training code runs, the owner confirms that LightGBM trained here is allowed under CLAUDE.md's AI-model rule, recorded in decisions.md (D30).
2. The model's input list is exactly the 31 features. (`tests/unit/test_model_inputs.py`)
3. Every training day is before every threshold day, every threshold day before every test day, and no transfer is in two sets. (`tests/unit/test_split.py`)
4. The threshold holds at most 1% of the threshold days' transfers, and within one transfer of 1% unless several transfers share the score at the threshold (D55). (`tests/unit/test_threshold.py`)
5. `make model` rebuilds the model file and the model card from the CSV, and two runs give the same metrics. (run by hand; card committed)
6. The model card states the data, split, inputs, threshold, PR-AUC, recall and fraud value caught at the 1% hold rate, false-positive rate, and limitations.

**Out of scope.** Hyperparameter sweeps, online retraining, deep learning.

## Phase 8 · Shadow and ship 2b (Feb 15–21)

**Scope.** The shadow scorer in the decision API (D39) with a migration for its columns; I8; the backtest CLI (D40q); the bench with shadow on and off; README's 2b section.

**Acceptance criteria**
1. I8: decisions are identical with shadow off, on, and on with a model that raises, returns nonsense or is slow. (`tests/invariants/test_i8_shadow.py`)
2. Each row stores the score and model name; a scoring failure stores null and is counted. (`tests/integration/test_shadow.py`)
3. The backtest CLI reports, on the test days, the rules at their own hold rate against the model at that rate, plus the model at 1%, with recall, fraud value caught and false-positive rate. (`tests/unit/test_backtest.py` on generated data; the real report committed)
4. With shadow on, p99 stays within 50 ms at every bench rate below the one where 2a's p99 crossed 50 ms, and the added latency comes from the report script. (`make bench`, `make check-readme`)
5. I6 to I8 run in CI.
6. README.md's 2b numbers come from the scripts, and PLAN.md section 9's definition of done is met.

**Out of scope.** Letting the model change live decisions; online retraining.
