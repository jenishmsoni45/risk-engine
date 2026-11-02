#!/usr/bin/env bash
# Phase 1, criterion 13 (D41): `make purge` also removes every image this repo pins that no
# container uses, and empties uv's cache. CI only: on a laptop it would remove images that other
# projects (tally) pull again, and the whole uv cache.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ "${CI:-}" != "true" ]; then
  echo "ci-check-purge.sh runs only in CI; run 'make purge' yourself to clean this machine" >&2
  exit 2
fi

fail() { echo "FAIL: $*" >&2; exit 1; }

# The images this repo pins, found the same way as the Makefile's purge target.
images=$( { docker compose --env-file .env.example config --images
            awk '/^FROM /{print $2}' Dockerfile; } | grep -vx 'risk-engine:local' | sort -u )
[ -n "$images" ] || fail "found no pinned images"
cache_dir=$(uv cache dir)

make purge

for image in $images; do
  in_use=$(docker ps --all --quiet --filter "ancestor=$image")
  if [ -z "$in_use" ] && docker image inspect "$image" > /dev/null 2>&1; then
    fail "$image is pinned, unused and still present"
  fi
done
# `uv cache clean` keeps the folder and its .lock file, so other uv processes waiting on the lock
# share it (uv 0.13.6, crates/uv-cache, Cache::clear). Anything else left is cached content.
if [ -d "$cache_dir" ]; then
  left=$(ls -A "$cache_dir" | grep -vx '.lock' || true)
  [ -z "$left" ] || fail "uv's cache at $cache_dir still holds: $(echo $left)"
fi
echo "make purge: OK"
