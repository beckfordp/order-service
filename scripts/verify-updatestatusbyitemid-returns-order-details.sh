#!/usr/bin/env bash
set -euo pipefail

# Verifies Phase 1 of US-5.3 (OrderStore.updateStatusByItemId returns order
# details): OrderStorePostgresSuite's Testcontainers tests already cover this
# through the Scala API, so the one thing worth confirming live, independent
# of the test suite, is that the raw SQL itself - the UPDATE ... RETURNING
# id, customer_id, total_cents statement - actually returns the right columns
# against a real running Postgres with the real migrated schema.
#
# Uses a standalone, throwaway Postgres container on an alternate port
# (15432) rather than this repo's own docker-compose.yml (port 5432) -
# avoids clashing with another service's docker-compose stack that may
# already be running on the standard port, and this check doesn't need
# Kafka/Redis/the Scala app at all, just the migrated schema.
#
# Usage: ./scripts/verify-updatestatusbyitemid-returns-order-details.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CONTAINER_NAME="order-service-verify-pg"
PORT=15432
FAILED=0

cleanup() {
  echo
  echo "Cleaning up..."
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "1. Starting a throwaway Postgres container on port ${PORT}..."
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
docker run -d --rm --name "$CONTAINER_NAME" \
  -e POSTGRES_DB=order -e POSTGRES_USER=order -e POSTGRES_PASSWORD=order \
  -p "${PORT}:5432" postgres:16-alpine >/dev/null

for _ in $(seq 1 60); do
  if docker exec "$CONTAINER_NAME" pg_isready -U order >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
echo "   OK: postgres ready"

psql_exec() {
  docker exec -i "$CONTAINER_NAME" psql -U order -d order -t -A -c "$1" \
    | grep -v -E '^(UPDATE|DELETE|INSERT|CREATE|ALTER) ' || true
}

echo
echo "2. Applying this repo's real Flyway migrations (V1-V5) directly..."
for f in src/main/resources/db/migration/V*.sql; do
  docker exec -i "$CONTAINER_NAME" psql -U order -d order -v ON_ERROR_STOP=1 < "$f" >/dev/null
done
echo "   OK: schema migrated"

check() {
  local label="$1" expected="$2" actual="$3"
  if [ "$actual" = "$expected" ]; then
    echo "   OK: ${label} (got ${actual})"
  else
    echo "   FAIL: ${label} - expected '${expected}', got '${actual}'" >&2
    FAILED=1
  fi
}

echo
echo "3. Inserting a Pending order + item directly via psql, then running the exact"
echo "   updateOrderStatusByItemId SQL from OrderStore.scala by hand..."

ORDER_ID="11111111-1111-1111-1111-111111111111"
ITEM_ID="22222222-2222-2222-2222-222222222222"

psql_exec "INSERT INTO \"order\" (id, customer_id, total_cents, status) VALUES ('${ORDER_ID}', 'cust-verify-1', 4242, 'pending');" >/dev/null
psql_exec "INSERT INTO order_items (id, order_id, sku, product_name, unit_price_cents, quantity) VALUES ('${ITEM_ID}', '${ORDER_ID}', 'sku-verify', 'Verify Widget', 2121, 2);" >/dev/null

RESULT="$(psql_exec "UPDATE \"order\" SET status = 'reserved', updated_at = now() WHERE status = 'pending' AND id = (SELECT order_id FROM order_items WHERE id = '${ITEM_ID}') RETURNING id, customer_id, total_cents;")"
echo "   Raw result: ${RESULT}"

RESULT_ID="$(echo "$RESULT" | cut -d'|' -f1)"
RESULT_CUSTOMER="$(echo "$RESULT" | cut -d'|' -f2)"
RESULT_TOTAL="$(echo "$RESULT" | cut -d'|' -f3)"

check "returned id matches the order" "$ORDER_ID" "$RESULT_ID"
check "returned customer_id" "cust-verify-1" "$RESULT_CUSTOMER"
check "returned total_cents" "4242" "$RESULT_TOTAL"

NEW_STATUS="$(psql_exec "SELECT status FROM \"order\" WHERE id = '${ORDER_ID}';")"
check "order actually transitioned to reserved" "reserved" "$NEW_STATUS"

echo
echo "4. Re-running the same UPDATE on the now-Reserved order confirms it's a no-op (0 rows)..."
REPEAT_RESULT="$(psql_exec "UPDATE \"order\" SET status = 'reserved', updated_at = now() WHERE status = 'pending' AND id = (SELECT order_id FROM order_items WHERE id = '${ITEM_ID}') RETURNING id;")"
check "no-op on an already-resolved order (empty result)" "" "$REPEAT_RESULT"

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
