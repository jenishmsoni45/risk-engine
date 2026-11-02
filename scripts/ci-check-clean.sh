#!/usr/bin/env bash
# Phase 1, criterion 12 (D41): `make clean` leaves no container, volume or network of this project,
# no image built from this repo and no .venv, and leaves data/, .env and every tracked file as they
# were. CI only: it writes a dummy .env and a file in data/, which must never replace the owner's.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ "${CI:-}" != "true" ]; then
  echo "ci-check-clean.sh runs only in CI: it writes .env and data/" >&2
  exit 2
fi

fail() { echo "FAIL: $*" >&2; exit 1; }
tracked_state() { git ls-files -z | xargs -0 git hash-object; }

mkdir -p data
printf 'kept by make clean\n' > data/keep.csv
cp .env.example .env
printf '# dummy .env written by ci-check-clean.sh\n' >> .env

data_before=$(git hash-object data/keep.csv)
env_before=$(git hash-object .env)
tracked_before=$(tracked_state)

# Something to remove: containers, the network, volumes, the built image and .venv.
make up
make sync
make clean

label="label=com.docker.compose.project=risk-engine"
[ -z "$(docker ps --all --quiet --filter "$label")" ] || fail "containers left"
[ -z "$(docker volume ls --quiet --filter "$label")" ] || fail "volumes left"
[ -z "$(docker network ls --quiet --filter "$label")" ] || fail "networks left"
if docker image inspect risk-engine:local > /dev/null 2>&1; then fail "risk-engine:local left"; fi
for path in .venv .pytest_cache .mypy_cache .ruff_cache .hypothesis; do
  [ ! -e "$path" ] || fail "$path left"
done
[ -z "$(find risk ledger_stub tests -name __pycache__)" ] || fail "__pycache__ left"

[ "$(git hash-object data/keep.csv)" = "$data_before" ] || fail "data/ changed"
[ "$(git hash-object .env)" = "$env_before" ] || fail ".env changed"
[ "$(tracked_state)" = "$tracked_before" ] || fail "a tracked file changed"
echo "make clean: OK"
