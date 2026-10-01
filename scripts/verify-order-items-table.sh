#!/usr/bin/env bash
set -euo pipefail

# Verifies the order_items table end-to-end (track order-items-table_20261001):
# brings up Postgres, runs migrations via a real sbt run, then confirms the
# table's schema and the (deliberate, for-now) absence of a FK constraint
# directly against the live database - not just that the test suite passes.
#
# Usage: ./scripts/verify-order-items-table.sh

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

psql_exec() {
  docker compose exec -T postgres psql -U order -d order -t -A -c "$1"
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
echo "2. Starting order-service (sbt run) in the background to apply migrations..."
if [ -z "${GITHUB_TOKEN:-}" ] || [ -z "${GITHUB_ACTOR:-}" ]; then
  echo "WARN: GITHUB_TOKEN / GITHUB_ACTOR not set - resolving purerestlib from" >&2
  echo "      GitHub Packages will fail without a read:packages token." >&2
fi
sbt run >/tmp/order-service-verify.log 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: order-service ready (migrations applied)"

echo
echo "3. Confirming order_items table has exactly the expected columns..."
columns="$(psql_exec "select string_agg(column_name, ',' order by ordinal_position) from information_schema.columns where table_name = 'order_items'")"
check "order_items columns" "id,order_id,sku,product_name,unit_price_cents,quantity" "$columns"

echo
echo "4. Confirming order_id has no FK constraint yet..."
fk_count="$(psql_exec "select count(*) from information_schema.table_constraints where table_name = 'order_items' and constraint_type = 'FOREIGN KEY'")"
check "no FK constraint on order_items" "0" "$fk_count"

echo
echo "5. Confirming an unrelated order_id is currently accepted (no FK enforced)..."
insert_result="$(psql_exec "insert into order_items (id, order_id, sku, product_name, unit_price_cents, quantity) values (gen_random_uuid(), gen_random_uuid(), 'sku-1', 'Widget', 999, 2); select 'ok'" | tail -1)"
check "insert with unrelated order_id succeeds" "ok" "$insert_result"

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
