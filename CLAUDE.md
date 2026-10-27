# Project 2: Streaming risk decision engine

A Python service that decides allow, hold or block for each transfer from the project 1 ledger within a latency budget, using rolling per-account features from the ledger's event stream. 2b adds a LightGBM model trained on PaySim and scored in shadow next to the rules.

## Source of truth

- Until the Phase 0 PR is merged, `PLAN.md` is the brief.
- After that, the docs are the source of truth: `docs/requirements.md`, `docs/architecture.md`, `docs/design.md`, `docs/decisions.md` and `docs/phases.md`. If code and docs disagree, the docs win. If the docs look wrong, stop and ask me.

## Workflow: one phase, one PR

I start every phase by name ("Start phase 3"). Never start a phase on your own.

1. **Sync.** `git checkout main && git pull`. Check with `gh pr list` that no earlier phase PR is still open. Create the branch `phase-<N>-<short-name>`.
2. **Read.** Read the phase in `docs/phases.md` and the docs it touches. If anything is ambiguous, missing or conflicts with the docs, ask me before writing code. Never invent requirements.
3. **Build the whole phase.** Write the code, a test for every acceptance criterion, and any doc updates the phase lists. Use small, logical commits with clear messages.
4. **Verify.** `make lint`, `make test` and `make audit` must pass, plus any check the phase names. Never skip, weaken or delete a test to get green; if a test is wrong, say so in the PR.
5. **Self-review.** Review `git diff main...HEAD` as a senior engineer would. Check correctness, failure handling, a test for each behaviour, security, and needless complexity. Fix what you find.
6. **Open the PR.** Push, then run `gh pr create` and fill in every section of `.github/pull_request_template.md`.
7. **Stop.** Send me the PR link and wait.

**When I ask you to address review comments,** read them with `gh pr view <n> --comments` and `gh api repos/{owner}/{repo}/pulls/<n>/comments`. Fix them on the same branch, run lint and tests, push, and reply with a list of what changed. Never merge.

## Phase 0 is planning only

Write no code and do no scaffolding. First ask me every open question: start with "Questions Phase 0 must settle" in PLAN.md, then add anything else unclear. Ask in batches of up to 5, each with options, trade-offs and your recommendation. Record every question and answer in `docs/decisions.md`. Only when nothing is open, write the Phase 0 docs listed in PLAN.md. Self-review them for gaps, contradictions and untestable criteria, then open a docs-only PR.

## Surprises mid-phase

If the design turns out to be wrong or a requirement can't be met, stop. Explain the problem and the options, then wait. Once I decide, record the decision in `docs/decisions.md` and continue. Out-of-scope ideas go under "Follow-ups" in the PR, not into the code.

## Hard limits

- Never push to `main`, merge a PR, force-push, or rewrite pushed history.
- I am the only author. Commits and PRs carry no `Co-Authored-By` trailer, no "Generated with Claude Code" line and no mention of Claude or AI in commit messages, PR titles or PR bodies.
- Free and local only. Everything runs in Docker Compose or on this machine. Use no cloud services (AWS, GCP, Azure, hosted databases or queues), no paid APIs, no sign-ups, and no credentials. If something seems to need one, stop and ask.
- No secrets in the repo. Use `.env.example` with dummy values.
- Never install anything on this Mac: no `brew`, no global `pip` or `npm`, no `curl | sh`, no installers. Project dependencies through uv are fine. If a tool is missing, stop and ask me.
- Add only well-known, widely used dependencies and Docker images (official or verified publisher), pinned to exact versions in the committed lockfile and compose file. Justify each new one in the PR.
- Never download or run an AI model. Nothing keeps running after a command finishes except the Docker Compose services, and `make down` stops those.
- Change `docs/requirements.md`, `docs/architecture.md` or `docs/design.md` only with my approval, and log each change in `docs/decisions.md`.
- Never put a number in `README.md` or `docs/` unless a script in the repo produced it.

## Project rules

- Python 3.12 with uv. ruff and mypy must be clean.
- The ledger is a separate repo (project 1, tally: `github.com/jenishmsoni45/tally`). Integration follows the contract in `docs/design.md`. Test against the ledger stub in this repo unless `docs/decisions.md` says otherwise. The stub's events must match tally's format exactly (topic `ledger.entries`, keyed by entry id, camelCase JSON). Never change the tally repo from here.
- Online and offline (2b) features come from one shared module. Never duplicate feature logic.
- PaySim (2b): I download the CSV into `data/` myself. `data/` is git-ignored. Never commit data or ask for Kaggle credentials.

## Stack and commands

- FastAPI, Redis, Kafka (`apache/kafka`, the same broker tally uses), Postgres, LightGBM (2b), k6, pytest + Hypothesis
- `make up` / `make down` · `make test` · `make lint` · `make audit` (pip-audit on the locked dependencies) · `make bench` (k6, results in `bench/results/`)

## Invariants the tests must enforce

1. The same requestId (the transfer's Idempotency-Key) always returns the same decision.
2. Duplicate or reordered events never change feature values.
3. Window values equal a recompute from the raw event log.
4. Past its timeout, the caller applies on_timeout and never hangs.
5. Every decision row stores the rule-set version, the fired rules and a feature snapshot.
6. (2b) Features use only events before the decision time.
7. (2b) Offline features equal the features logged online.
8. (2b) Shadow scoring never changes a live decision.
