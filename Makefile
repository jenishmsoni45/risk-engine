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

.PHONY: help sync up down test lint audit clean purge

help:
	@echo "make up      start the stack and wait until every service is healthy"
	@echo "make down    stop the stack and remove its containers (data volumes stay)"
	@echo "make test    start the stack, then run every test"
	@echo "make lint    lock check, ruff check, ruff format check, mypy"
	@echo "make audit   pip-audit on the locked dependencies"
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

audit: sync
	@tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT; \
	uv export --locked --all-groups --no-emit-project --format requirements.txt \
		--output-file "$$tmp/requirements.txt" --quiet; \
	uv run --locked pip-audit --strict --disable-pip --require-hashes \
		--requirement "$$tmp/requirements.txt"

# Never touches data/, .env or any tracked file (D41).
clean:
	$(COMPOSE) down --volumes --remove-orphans
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
