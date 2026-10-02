#!/usr/bin/env bash
set -euo pipefail

# Verifies Phase 2 of US-8.1 (Redis config + OrderHistoryCache): the cache
# isn't wired into Main.scala or any HTTP endpoint yet (that's Phase 3), and
# OrderHistoryCache's own get/set/TTL behavior is already covered against a
# real Redis by OrderHistoryCacheSuite's Testcontainers tests - so the one
# thing worth confirming live is that order-service still boots correctly
# now that OrderServiceConfig has a new required redis block (a
# config-loading mistake here would only surface at startup, not in the test
# suite's own ConfigSource tests, which construct their own HOCON).
#
# Usage: ./scripts/verify-redis-config.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

FAILED=0
SBT_PID=""

cleanup() {
  echo
  echo "Cleaning up..."
  [ -n "$SBT_PID" ] && kill "$SBT_PID" >/dev/null 2>&1 || true
  docker compose down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_ready() {
  for _ in $(seq 1 90); do
    status="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/health || true)"
    [ "$status" = "200" ] && return 0
    sleep 1
  done
  echo "FAIL: order-service did not become ready in time." >&2
  return 1
}

check() {
  local label="$1" expected="$2" actual="$3"
  if [ "$actual" = "$expected" ]; then
    echo "   OK: ${label} (got ${actual})"
  else
    echo "   FAIL: ${label} - expected ${expected}, got ${actual}" >&2
    FAILED=1
  fi
}

echo "1. docker compose up -d (fresh volumes)..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d
echo "   OK: postgres + kafka + redis starting"

echo
echo "2. Starting order-service (sbt run) - confirms OrderServiceConfig (now requiring redis) loads cleanly..."
if [ -z "${GITHUB_TOKEN:-}" ] || [ -z "${GITHUB_ACTOR:-}" ]; then
  echo "WARN: GITHUB_TOKEN / GITHUB_ACTOR not set - resolving purerestlib from" >&2
  echo "      GitHub Packages will fail without a read:packages token." >&2
fi
sbt run >/tmp/order-service-verify.log 2>&1 &
SBT_PID=$!
if wait_ready; then
  check "order-service booted with the new redis config" "200" "200"
else
  echo "   FAIL: order-service failed to start - check /tmp/order-service-verify.log" >&2
  tail -40 /tmp/order-service-verify.log >&2 || true
  FAILED=1
fi

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
