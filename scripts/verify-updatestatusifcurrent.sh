#!/usr/bin/env bash
set -euo pipefail

# Verifies Phase 1 of US-6.3 (OrderStatus.Confirmed/PaymentFailed +
# OrderStore.updateStatusIfCurrent): OrderStorePostgresSuite's Testcontainers
# tests already cover this through the Scala API, so the one thing worth
# confirming live, independent of the test suite, is that the raw SQL
# itself - the V6 migration's widened CHECK constraint, and the
# updateOrderStatusIfCurrent UPDATE ... WHERE id = ? AND status = ? ...
# RETURNING statement - actually behaves as expected against a real running
# Postgres with the real migrated schema.
#
# Uses a standalone, throwaway Postgres container on an alternate port
# (15432) rather than this repo's own docker-compose.yml (port 5432) -
# avoids clashing with another service's docker-compose stack that may
# already be running on the standard port, and this check doesn't need
# Kafka/Redis/the Scala app at all, just the migrated schema.
#
# Usage: ./scripts/verify-updatestatusifcurrent.sh

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
echo "2. Applying this repo's real Flyway migrations (V1-V6) directly..."
for f in src/main/resources/db/migration/V*.sql; do
  docker exec -i "$CONTAINER_NAME" psql -U order -d order -v ON_ERROR_STOP=1 < "$f" >/dev/null
done
echo "   OK: schema migrated (V6 widens order_status_check to 5 values)"

check() {
  local label="$1" expected="$2" actual="$3"
  if [ "$actual" = "$expected" ]; then
    echo "   OK: ${label} (got ${actual})"
  else
    echo "   FAIL: ${label} - expected '${expected}', got '${actual}'" >&2
    FAILED=1
  fi
}

ORDER_ID="33333333-3333-3333-3333-333333333333"

echo
echo "3. The CHECK constraint accepts both new status values directly..."
psql_exec "INSERT INTO \"order\" (id, customer_id, total_cents, status) VALUES ('${ORDER_ID}', 'cust-verify-2', 4242, 'reserved');" >/dev/null
CONFIRMED_INSERT_OK="$(psql_exec "UPDATE \"order\" SET status = 'confirmed' WHERE id = '${ORDER_ID}' RETURNING status;")"
check "CHECK constraint accepts 'confirmed'" "confirmed" "$CONFIRMED_INSERT_OK"
PAYMENT_FAILED_INSERT_OK="$(psql_exec "UPDATE \"order\" SET status = 'payment_failed' WHERE id = '${ORDER_ID}' RETURNING status;")"
check "CHECK constraint accepts 'payment_failed'" "payment_failed" "$PAYMENT_FAILED_INSERT_OK"

echo
echo "4. A value outside the closed set is still rejected (CHECK constraint intact)..."
set +e
BAD_INSERT_ERR="$(docker exec -i "$CONTAINER_NAME" psql -U order -d order -c "UPDATE \"order\" SET status = 'bogus' WHERE id = '${ORDER_ID}';" 2>&1)"
set -e
if echo "$BAD_INSERT_ERR" | grep -q "order_status_check"; then
  echo "   OK: rejected an out-of-set status value (constraint violation raised)"
else
  echo "   FAIL: expected a order_status_check violation, got: ${BAD_INSERT_ERR}" >&2
  FAILED=1
fi

echo
echo "5. Running the exact updateOrderStatusIfCurrent SQL from OrderStore.scala by hand,"
echo "   resetting the order to 'reserved' first..."
psql_exec "UPDATE \"order\" SET status = 'reserved' WHERE id = '${ORDER_ID}';" >/dev/null

RESULT="$(psql_exec "UPDATE \"order\" SET status = 'confirmed', updated_at = now() WHERE id = '${ORDER_ID}' AND status = 'reserved' RETURNING id, customer_id, total_cents;")"
echo "   Raw result: ${RESULT}"

RESULT_ID="$(echo "$RESULT" | cut -d'|' -f1)"
RESULT_CUSTOMER="$(echo "$RESULT" | cut -d'|' -f2)"
RESULT_TOTAL="$(echo "$RESULT" | cut -d'|' -f3)"

check "returned id matches the order" "$ORDER_ID" "$RESULT_ID"
check "returned customer_id" "cust-verify-2" "$RESULT_CUSTOMER"
check "returned total_cents" "4242" "$RESULT_TOTAL"

NEW_STATUS="$(psql_exec "SELECT status FROM \"order\" WHERE id = '${ORDER_ID}';")"
check "order actually transitioned to confirmed" "confirmed" "$NEW_STATUS"

echo
echo "6. Re-running the same guarded UPDATE (still expecting 'reserved') is a no-op (0 rows),"
echo "   since the order is now 'confirmed' - proves a redelivered/out-of-order event can't"
echo "   regress an already-settled order..."
REPEAT_RESULT="$(psql_exec "UPDATE \"order\" SET status = 'confirmed', updated_at = now() WHERE id = '${ORDER_ID}' AND status = 'reserved' RETURNING id;")"
check "no-op when current status no longer matches expected (empty result)" "" "$REPEAT_RESULT"

STILL_CONFIRMED="$(psql_exec "SELECT status FROM \"order\" WHERE id = '${ORDER_ID}';")"
check "order status unchanged by the no-op" "confirmed" "$STILL_CONFIRMED"

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
