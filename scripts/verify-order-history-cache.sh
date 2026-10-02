#!/usr/bin/env bash
set -euo pipefail

# Verifies US-8.1 (track order-history-cache_20261002) end-to-end: brings up
# Postgres + Kafka + Redis + a real sbt run instance, drives real checkouts
# and GET /orders calls, and inspects Redis directly (via its own redis-cli,
# not the application) to confirm the cache is genuinely populated and that
# the TTL genuinely expires it server-side - not just that the test suite's
# Testcontainers-backed OrderHistoryCacheSuite passes.
#
# Usage: ./scripts/verify-order-history-cache.sh

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

redis_get() {
  docker compose exec -T redis redis-cli get "$1"
}

echo "1. docker compose up -d (fresh volumes, postgres + kafka + redis)..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d
echo "   OK: postgres + kafka + redis starting"

echo
echo "2. Starting order-service (sbt run) with a 2s history cache TTL, pointed at an unreachable inventory-service (irrelevant to this check)..."
if [ -z "${GITHUB_TOKEN:-}" ] || [ -z "${GITHUB_ACTOR:-}" ]; then
  echo "WARN: GITHUB_TOKEN / GITHUB_ACTOR not set - resolving purerestlib from" >&2
  echo "      GitHub Packages will fail without a read:packages token." >&2
fi
INVENTORY_SERVICE_URL="http://localhost:19999" ORDER_HISTORY_CACHE_TTL_SECONDS=2 \
  sbt run >/tmp/order-service-verify.log 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: order-service ready on :8080"

echo
echo "3. Checking out two orders for the same customer..."
customer_id="cust-$(date +%s)"
order1_id="$(curl -s -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d "{\"customerId\":\"${customer_id}\",\"items\":[{\"sku\":\"sku-1\",\"productName\":\"Widget\",\"unitPriceCents\":999,\"quantity\":2}]}" \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")"
order2_id="$(curl -s -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d "{\"customerId\":\"${customer_id}\",\"items\":[{\"sku\":\"sku-2\",\"productName\":\"Gadget\",\"unitPriceCents\":500,\"quantity\":1}]}" \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")"
echo "   order1=${order1_id}, order2=${order2_id}, customer=${customer_id}"

echo
echo "4. GET /orders?customerId=... returns both orders, newest-first..."
list_response="$(curl -s -w '\n%{http_code}' "http://localhost:8080/orders?customerId=${customer_id}")"
list_status="$(echo "$list_response" | tail -1)"
list_body="$(echo "$list_response" | sed '$d')"
check "GET /orders returns 200" "200" "$list_status"
returned_ids="$(echo "$list_body" | python3 -c "import json,sys; print(','.join(o['id'] for o in json.load(sys.stdin)))")"
check "returns both orders newest-first" "${order2_id},${order1_id}" "$returned_ids"

echo
echo "5. Inspecting Redis directly - the cache key should now hold both orders..."
cached_raw="$(redis_get "order-history:${customer_id}")"
cached_ids="$(echo "$cached_raw" | python3 -c "import json,sys; print(','.join(o['id'] for o in json.load(sys.stdin)))" 2>/dev/null || echo "<unparseable>")"
check "Redis cache key holds both orders newest-first" "${order2_id},${order1_id}" "$cached_ids"

echo
echo "6. Waiting past the 2s TTL, then confirming Redis expired the key..."
sleep 3
expired_raw="$(redis_get "order-history:${customer_id}")"
check "Redis cache key expired" "" "$expired_raw"

echo
echo "7. GET /orders still works after the cache expired (re-populates from the store)..."
after_expiry_status="$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:8080/orders?customerId=${customer_id}")"
check "GET /orders returns 200 after cache expiry" "200" "$after_expiry_status"
recached_raw="$(redis_get "order-history:${customer_id}")"
check "Redis cache key is repopulated" "1" "$([ -n "$recached_raw" ] && echo 1 || echo 0)"

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
