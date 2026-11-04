# The risk engine's commands (CLAUDE.md). Every install uses `uv sync --locked` or
# `uv run --locked`, which refuse a lock that doesn't match pyproject.toml (D41).
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

# Compose reads .env when it exists and the throwaway values in .env.example otherwise.
ENV_FILE := $(if $(wildcard .env),.env,.env.example)
COMPOSE := docker compose --env-file $(ENV_FILE)
APP_IMAGE := risk-engine:local
PY_DIRS := $(wildcard risk ledger_stub tests ml bench)
# The throwaway stacks of make bench, make e2e and make parity (D48.6, D49, D52a), removed when
# they end.
THROWAWAY_PROJECTS := risk-engine-bench risk-engine-e2e risk-engine-parity
# The owner's PaySim copy, downloaded by hand into the git-ignored data/ (CLAUDE.md).
PAYSIM_CSV ?= data/PS_20174392719_1491204439457_log.csv

.PHONY: help sync up down test lint audit clean purge rebuild-features bench report check-readme e2e \
	profile paysim-rules parity

help:
	@echo "make up      start the stack and wait until every service is healthy"
	@echo "make down    stop the stack and remove its containers (data volumes stay)"
	@echo "make test    start the stack, then run every test"
	@echo "make lint    lock check, ruff check, ruff format check, mypy"
	@echo "make audit   pip-audit on the locked dependencies"
	@echo "make rebuild-features   refill Redis from ledger.entries and the decision log"
	@echo "make bench   the k6 scenarios on a throwaway stack, into bench/results/<UTC date>/"
	@echo "make report  rewrite README.md's bench tables from the newest results"
	@echo "make check-readme   fail if README.md's bench tables differ from make report's"
	@echo "make e2e     real tally (TALLY_DIR, default ~/Desktop/tally) against a throwaway stack"
	@echo "make profile   profile PAYSIM_CSV into ml/reports/paysim_profile.md"
	@echo "make paysim-rules   write ml/rules/paysim.yaml, D20's rules rescaled to PAYSIM_CSV (D54)"
	@echo "make parity  replay PAYSIM_CSV's first 10,000 transfers on a throwaway stack; check I7"
	@echo "make clean   remove this project's containers, volumes, network, image, .venv and caches"
	@echo "make purge   make clean, then the pinned images no container uses and uv's cache"

sync:
	uv sync --locked

up:
	$(COMPOSE) up --detach --build --wait --wait-timeout 300

down:
	$(COMPOSE) down --remove-orphans

test: up sync
	uv run --locked pytest

lint: sync
	uv lock --check
	uv run --locked ruff check .
	uv run --locked ruff format --check .
	uv run --locked mypy

# Redis holds nothing that can't be rebuilt (design.md 3.5): runs in a one-off container from the
# consumer's service, so it reaches the same Redis, Postgres and Kafka as the consumer does.
rebuild-features:
	$(COMPOSE) run --rm --no-deps --no-TTY consumer python -m risk.features.rebuild

# The measurement plan (design.md section 12). Needs the normal stack stopped (`make down`).
bench: sync
	uv run --locked python -m bench.run

report: sync
	uv run --locked python -m bench.report --write README.md

check-readme: sync
	uv run --locked python -m bench.report --check README.md

# Real tally against this service (D26, D48.6). Local only; needs the normal stack stopped.
e2e: sync
	uv run --locked python -m scripts.e2e

# 2b (design.md section 8). The profile confirms the sample before Phase 7 (D32).
profile: sync
	uv run --locked python -m ml.profile --csv "$(PAYSIM_CSV)"

# The rule set the backtest compares with the model (D54); the service never loads it.
paysim-rules: sync
	uv run --locked python -m ml.paysim_rules --csv "$(PAYSIM_CSV)"

# I7 on real data (D36, D37): local only; needs the normal stack stopped (`make down`).
parity: sync
	uv run --locked python -m ml.parity --csv "$(PAYSIM_CSV)"

audit: sync
	@tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT; \
	uv export --locked --all-groups --no-emit-project --format requirements.txt \
		--output-file "$$tmp/requirements.txt" --quiet; \
	uv run --locked pip-audit --strict --disable-pip --require-hashes \
		--requirement "$$tmp/requirements.txt"

# Never touches data/, .env or any tracked file (D41).
clean:
	$(COMPOSE) down --volumes --remove-orphans
	@for project in $(THROWAWAY_PROJECTS); do \
		$(COMPOSE) --project-name "$$project" down --volumes --remove-orphans; \
	done
	@if docker image inspect $(APP_IMAGE) > /dev/null 2>&1; then docker image rm $(APP_IMAGE); fi
	rm -rf .venv .pytest_cache .mypy_cache .ruff_cache .hypothesis
	find $(PY_DIRS) -type d -name __pycache__ -prune -exec rm -rf {} +

# Docker refuses to remove an image that any container uses, so tally's containers keep theirs (D41).
purge: clean
	@for image in $$( { $(COMPOSE) config --images; awk '/^FROM /{print $$2}' Dockerfile; } \
			| grep -vx '$(APP_IMAGE)' | sort -u); do \
		if docker image inspect "$$image" > /dev/null 2>&1; then \
			docker image rm "$$image" || echo "kept $$image: a container still uses it"; \
		fi; \
	done
	uv cache clean
	@echo
	@echo "Two steps are left to you, in your own terminal (D42):"
	@echo "  docker builder prune   # Docker's build cache, shared by every project on this Mac"
	@echo "  brew uninstall uv"
