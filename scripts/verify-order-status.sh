#!/usr/bin/env bash
set -euo pipefail

# Verifies the OrderStatus hardening end-to-end (track status-enum_20261001):
# brings up Postgres + the real service, then drives real HTTP traffic through
# it to confirm valid statuses still round-trip and invalid ones are rejected
# with 400 and persist nothing — not just that the unit/integration test
# suite passes.
#
# Usage: ./scripts/verify-order-status.sh

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
echo "3. POST /orders with a valid body..."
create_response="$(curl -s -w '\n%{http_code}' -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" -d '{"customerId":"cust-123","totalCents":4999}')"
create_status="$(echo "$create_response" | tail -1)"
create_body="$(echo "$create_response" | sed '$d')"
check "POST /orders returns 201" "201" "$create_status"
order_id="$(echo "$create_body" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")"
status_field="$(echo "$create_body" | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")"
check "created order has status pending" "pending" "$status_field"

echo
echo "4. PATCH with an invalid status is rejected and persists nothing..."
invalid_status="$(curl -s -o /dev/null -w '%{http_code}' -X PATCH "http://localhost:8080/orders/${order_id}" \
  -H "Content-Type: application/json" -d '{"status":"bogus"}')"
check "PATCH with invalid status returns 400" "400" "$invalid_status"
unchanged="$(curl -s "http://localhost:8080/orders/${order_id}" | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")"
check "status unchanged after rejected PATCH" "pending" "$unchanged"

echo
echo "5. PATCH with a valid status succeeds..."
valid_patch="$(curl -s -w '\n%{http_code}' -X PATCH "http://localhost:8080/orders/${order_id}" \
  -H "Content-Type: application/json" -d '{"status":"reserved"}')"
valid_patch_status="$(echo "$valid_patch" | tail -1)"
valid_patch_body="$(echo "$valid_patch" | sed '$d')"
check "PATCH with valid status returns 200" "200" "$valid_patch_status"
new_status="$(echo "$valid_patch_body" | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")"
check "status updated to reserved" "reserved" "$new_status"

echo
echo "6. PUT with an invalid status is rejected..."
invalid_put="$(curl -s -o /dev/null -w '%{http_code}' -X PUT "http://localhost:8080/orders/${order_id}" \
  -H "Content-Type: application/json" -d '{"status":"also-bogus"}')"
check "PUT with invalid status returns 400" "400" "$invalid_put"

echo
echo "7. DELETE, then GET returns 404..."
delete_status="$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "http://localhost:8080/orders/${order_id}")"
check "DELETE returns 204" "204" "$delete_status"
get_after_delete="$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:8080/orders/${order_id}")"
check "GET after delete returns 404" "404" "$get_after_delete"

echo
echo "8. Health and docs endpoints..."
health="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/health)"
check "GET /health returns 200" "200" "$health"
ready="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/health/ready)"
check "GET /health/ready returns 200" "200" "$ready"
docs="$(curl -s -L -o /dev/null -w '%{http_code}' http://localhost:8080/docs)"
check "GET /docs returns 200" "200" "$docs"

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
