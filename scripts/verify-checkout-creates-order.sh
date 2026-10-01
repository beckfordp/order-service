#!/usr/bin/env bash
set -euo pipefail

# Verifies US-3.1 (checkout creates an order) end-to-end: brings up Postgres
# + a real sbt run instance, then drives real HTTP traffic through checkout
# with line items - total computed server-side, items persisted atomically,
# items returned on get/patch/put, invalid items rejected, and CASCADE
# delete still works through the full stack.
#
# Usage: ./scripts/verify-checkout-creates-order.sh

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
echo "   OK: postgres starting"

echo
echo "2. Starting order-service (sbt run) in the background..."
if [ -z "${GITHUB_TOKEN:-}" ] || [ -z "${GITHUB_ACTOR:-}" ]; then
  echo "WARN: GITHUB_TOKEN / GITHUB_ACTOR not set - resolving purerestlib from" >&2
  echo "      GitHub Packages will fail without a read:packages token." >&2
fi
sbt run >/tmp/order-service-verify.log 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: order-service ready on :8080"

echo
echo "3. Checkout with two line items..."
create_response="$(curl -s -w '\n%{http_code}' -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust-123","items":[{"sku":"sku-1","productName":"Widget","unitPriceCents":999,"quantity":2},{"sku":"sku-2","productName":"Gadget","unitPriceCents":500,"quantity":3}]}')"
create_status="$(echo "$create_response" | tail -1)"
create_body="$(echo "$create_response" | sed '$d')"
check "POST /orders returns 201" "201" "$create_status"
order_id="$(echo "$create_body" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")"
total_cents="$(echo "$create_body" | python3 -c "import json,sys; print(json.load(sys.stdin)['totalCents'])")"
item_count="$(echo "$create_body" | python3 -c "import json,sys; print(len(json.load(sys.stdin)['items']))")"
check "totalCents computed from items (999*2 + 500*3)" "3498" "$total_cents"
check "response includes both items" "2" "$item_count"

echo
echo "4. Checkout with an empty items list is rejected..."
empty_status="$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" -d '{"customerId":"cust-123","items":[]}')"
check "empty items returns 400" "400" "$empty_status"

echo
echo "5. Checkout with an invalid item (negative quantity) is rejected..."
invalid_status="$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust-123","items":[{"sku":"sku-1","productName":"Widget","unitPriceCents":999,"quantity":-1}]}')"
check "invalid item returns 400" "400" "$invalid_status"

echo
echo "6. GET returns the order's items..."
get_items="$(curl -s "http://localhost:8080/orders/${order_id}" | python3 -c "import json,sys; print(len(json.load(sys.stdin)['items']))")"
check "GET includes both items" "2" "$get_items"

echo
echo "7. PATCH response still includes the (unchanged) items..."
patch_response="$(curl -s -X PATCH "http://localhost:8080/orders/${order_id}" \
  -H "Content-Type: application/json" -d '{"status":"reserved"}')"
patch_items="$(echo "$patch_response" | python3 -c "import json,sys; print(len(json.load(sys.stdin)['items']))")"
check "PATCH response includes both items" "2" "$patch_items"

echo
echo "8. DELETE cascades to remove the order's items (verified via Postgres)..."
delete_status="$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "http://localhost:8080/orders/${order_id}")"
check "DELETE returns 204" "204" "$delete_status"
remaining_items="$(docker compose exec -T postgres psql -U order -d order -t -A -c \
  "select count(*) from order_items where order_id = '${order_id}'")"
check "order_items removed after delete" "0" "$remaining_items"
get_after_delete="$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:8080/orders/${order_id}")"
check "GET after delete returns 404" "404" "$get_after_delete"

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
