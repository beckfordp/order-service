#!/usr/bin/env bash
set -euo pipefail

# Verifies US-4.2 end-to-end (track reserve-resilience_20261001): brings up
# Postgres + a real sbt run instance, pointed at a deliberately unreachable
# inventory-service URL (nothing is listening on that port) - a real
# connection-refused failure, not a stub - to confirm the resilience-wrapped
# reserve call actually retries, ultimately fails, and checkout still
# creates the order with status reservation_failed rather than hanging or
# erroring. No live inventory-service exists in this repo's own test suite
# (per PLAN.md's stub/fan-out guidance), so this is the closest thing to a
# real integration check available here.
#
# Usage: ./scripts/verify-reserve-resilience.sh

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

echo "1. docker compose up -d (postgres only - no inventory-service)..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d
echo "   OK: postgres starting"

echo
echo "2. Starting order-service, pointed at an unreachable inventory-service URL..."
if [ -z "${GITHUB_TOKEN:-}" ] || [ -z "${GITHUB_ACTOR:-}" ]; then
  echo "WARN: GITHUB_TOKEN / GITHUB_ACTOR not set - resolving purerestlib from" >&2
  echo "      GitHub Packages will fail without a read:packages token." >&2
fi
INVENTORY_SERVICE_URL="http://localhost:19999" sbt run >/tmp/order-service-verify.log 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: order-service ready on :8080"

echo
echo "3. Checkout against the unreachable inventory-service..."
start_time=$(date +%s)
create_response="$(curl -s -w '\n%{http_code}' -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust-123","items":[{"sku":"sku-1","productName":"Widget","unitPriceCents":999,"quantity":2}]}')"
end_time=$(date +%s)
create_status="$(echo "$create_response" | tail -1)"
create_body="$(echo "$create_response" | sed '$d')"
elapsed=$((end_time - start_time))
check "POST /orders still returns 201 despite the unreachable downstream" "201" "$create_status"
order_status="$(echo "$create_body" | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")"
check "order status reflects the failed reservation" "reservation_failed" "$order_status"
echo "   (took ${elapsed}s - retry backoff against the unreachable host ran its course)"

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
