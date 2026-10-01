#!/usr/bin/env bash
set -euo pipefail

# Verifies the order_items FK constraint end-to-end (track order-items-fk_20261001):
# brings up Postgres, runs migrations via a real sbt run, then confirms the
# FK rejects an unrelated order_id and that ON DELETE CASCADE actually
# removes order_items rows when the parent order is deleted - directly
# against the live database, not just via the test suite.
#
# Usage: ./scripts/verify-order-items-fk.sh

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
echo "3. Confirming the FK constraint exists on order_items.order_id..."
fk_count="$(psql_exec "select count(*) from information_schema.table_constraints where table_name = 'order_items' and constraint_type = 'FOREIGN KEY'")"
check "FK constraint present" "1" "$fk_count"

echo
echo "4. Confirming an unrelated order_id is now rejected..."
set +e
reject_output="$(psql_exec "insert into order_items (id, order_id, sku, product_name, unit_price_cents, quantity) values (gen_random_uuid(), gen_random_uuid(), 'sku-1', 'Widget', 999, 2)" 2>&1)"
reject_exit=$?
set -e
if [ "$reject_exit" -ne 0 ] && echo "$reject_output" | grep -qi "foreign key"; then
  echo "   OK: unrelated order_id rejected with a foreign key violation"
else
  echo "   FAIL: expected a foreign key violation, got exit=${reject_exit}: ${reject_output}" >&2
  FAILED=1
fi

echo
echo "5. Confirming ON DELETE CASCADE actually removes order_items rows..."
order_id="$(psql_exec "select gen_random_uuid()")"
item_id="$(psql_exec "select gen_random_uuid()")"
psql_exec "insert into \"order\" (id, customer_id, total_cents, status) values ('${order_id}', 'cust-1', 100, 'pending')" >/dev/null
psql_exec "insert into order_items (id, order_id, sku, product_name, unit_price_cents, quantity) values ('${item_id}', '${order_id}', 'sku-1', 'Widget', 999, 2)" >/dev/null
psql_exec "delete from \"order\" where id = '${order_id}'" >/dev/null
remaining="$(psql_exec "select count(*) from order_items where id = '${item_id}'")"
check "order_item removed after parent order deleted" "0" "$remaining"

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
