#!/usr/bin/env bash
set -euo pipefail

# Verifies Checkout Journey 1 (checkout-journey1_20261027; US-11.1, US-14.2,
# TD-7.3, ADR 0018) end-to-end: the real order-service + Postgres, with
# scripts/lib/checkout-stubs.py standing in for inventory-service and
# payment-service. POST /orders reserves the whole order, authorizes, and only
# then writes the order row; every failure after a successful reserve
# releases it, and no failure leaves a row.
#
# Usage: ./scripts/verify-checkout-journey1.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

STUB_PORT=18081
CALLS="$(mktemp -t checkout-calls)"
SBT_LOG=/tmp/order-service-verify-checkout-journey1.log
DECLINED_METHOD="00000000-0000-0000-0000-000000000402"
UNKNOWN_METHOD="00000000-0000-0000-0000-000000000404"
GOOD_METHOD="6f1c2c1e-6a43-4d2b-9a55-2f3f6c1b9b10"
FAILED=0
SBT_PID=""
STUB_PID=""

cleanup() {
  echo
  echo "Cleaning up..."
  [ -n "$SBT_PID" ] && kill -9 "$SBT_PID" >/dev/null 2>&1 || true
  [ -n "$STUB_PID" ] && kill -9 "$STUB_PID" >/dev/null 2>&1 || true
  lsof -ti:8080 | xargs kill -9 >/dev/null 2>&1 || true
  rm -f "$CALLS"
  docker compose down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_ready() {
  for _ in $(seq 1 120); do
    status="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/health || true)"
    [ "$status" = "200" ] && return 0
    sleep 1
  done
  echo "FAIL: order-service did not become ready in time (see $SBT_LOG)." >&2
  exit 1
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

psql_q() {
  docker compose exec -T postgres psql -U order -d order -tAc "$1"
}

json_field() {
  python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"
}

# order <sku> <paymentMethodId> -> request body
order() {
  printf '{"customerId":"verify-j1","items":[{"sku":"%s","productName":"Widget","unitPriceCents":1250,"quantity":2}],"shippingAddress":{"line1":"1 High St","city":"London","postalCode":"N1 1AA","country":"GB"},"paymentMethodId":"%s"}' "$1" "$2"
}

# checkout <body>: sets STATUS, BODY and NEW_CALLS (stub calls it caused, as
# "call:orderId" lines)
checkout() {
  local before response
  before="$(wc -l <"$CALLS" | tr -d ' ')"
  response="$(curl -s -w '\n%{http_code}' -X POST http://localhost:8080/orders \
    -H "Content-Type: application/json" -d "$1")"
  STATUS="$(echo "$response" | tail -1)"
  BODY="$(echo "$response" | sed '$d')"
  NEW_CALLS="$(tail -n +"$((before + 1))" "$CALLS" | json_lines)"
}

json_lines() {
  python3 -c "
import json,sys
for l in sys.stdin:
    c=json.loads(l); print(c['call']+':'+c['orderId'])"
}

calls_shape() { echo "$NEW_CALLS" | cut -d: -f1 | tr '\n' ' ' | sed 's/ $//'; }
same_order() { echo "$NEW_CALLS" | cut -d: -f2 | sort -u | wc -l | tr -d ' '; }
rows_for() { psql_q "SELECT count(*) FROM \"order\" WHERE id = '$1'"; }

echo "1. docker compose up -d (fresh volumes)..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d
echo "   waiting for kafka to be healthy..."
for _ in $(seq 1 60); do
  health="$(docker compose ps --format json kafka 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('Health',''))" 2>/dev/null || true)"
  [ "$health" = "healthy" ] && break
  sleep 1
done

echo
echo "2. Starting the inventory/payment stand-in on :${STUB_PORT}..."
python3 scripts/lib/checkout-stubs.py "$STUB_PORT" "$CALLS" </dev/null >/dev/null 2>&1 &
STUB_PID=$!
sleep 1
echo "   OK: stand-in running"

echo
echo "3. Starting order-service (sbt run), pointed at the stand-in..."
export GITHUB_ACTOR="${GITHUB_ACTOR:-beckfordp}"
export GITHUB_TOKEN="${GITHUB_TOKEN:-$(gh auth token)}"
INVENTORY_SERVICE_URL="http://localhost:${STUB_PORT}" \
  PAYMENT_SERVICE_URL="http://localhost:${STUB_PORT}" \
  sbt -batch run </dev/null >"$SBT_LOG" 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: order-service ready on :8080"

echo
echo "4. Authorized checkout -> 201, authorized, address stored, reserve then authorize..."
checkout "$(order sku-ok "$GOOD_METHOD")"
check "status" "201" "$STATUS"
order_id="$(echo "$BODY" | json_field "d['id']")"
check "order status" "authorized" "$(echo "$BODY" | json_field "d['status']")"
check "shipping address echoed" "London" "$(echo "$BODY" | json_field "d['shippingAddress']['city']")"
check "stub calls" "reserve authorize" "$(calls_shape)"
check "calls carry the returned order id" "reserve:${order_id}" "$(echo "$NEW_CALLS" | head -1)"
check "order row" "authorized|2500" "$(psql_q "SELECT status || '|' || total_cents FROM \"order\" WHERE id = '${order_id}'")"
check "shipping_address column" "N1 1AA" "$(psql_q "SELECT shipping_address->>'postalCode' FROM \"order\" WHERE id = '${order_id}'")"
check "no outbox row for checkout" "0" "$(psql_q "SELECT count(*) FROM outbox_events WHERE payload->>'orderId' = '${order_id}'")"
check "GET /orders/{id}" "authorized" "$(curl -s "http://localhost:8080/orders/${order_id}" | json_field "d['status']")"

echo
echo "5. Declined payment -> 402, reservation released, no row..."
checkout "$(order sku-ok "$DECLINED_METHOD")"
check "status" "402" "$STATUS"
check "reason" "insufficient_funds" "$(echo "$BODY" | json_field "d['paymentFailure']['reason']")"
check "stub calls" "reserve authorize release" "$(calls_shape)"
check "one order id throughout" "1" "$(same_order)"
check "no order row" "0" "$(rows_for "$(echo "$NEW_CALLS" | head -1 | cut -d: -f2)")"

echo
echo "6. Unknown payment method -> 400, reservation released, no row..."
checkout "$(order sku-ok "$UNKNOWN_METHOD")"
check "status" "400" "$STATUS"
check "error" "Payment method not found" "$(echo "$BODY" | json_field "d['error']")"
check "stub calls" "reserve authorize release" "$(calls_shape)"
check "no order row" "0" "$(rows_for "$(echo "$NEW_CALLS" | head -1 | cut -d: -f2)")"

echo
echo "7. Out of stock -> 409 with failures, no authorize, no release, no row..."
checkout "$(order oos-widget "$GOOD_METHOD")"
check "status" "409" "$STATUS"
check "failure" "oos-widget:insufficient_stock" "$(echo "$BODY" | json_field "d['reservationFailures'][0]['sku']+':'+d['reservationFailures'][0]['reason']")"
check "stub calls" "reserve" "$(calls_shape)"
check "no order row" "0" "$(rows_for "$(echo "$NEW_CALLS" | head -1 | cut -d: -f2)")"

echo
echo "8. Invalid checkout (missing address fields) -> 400, no downstream calls..."
checkout '{"customerId":"verify-j1","items":[{"sku":"sku-ok","productName":"Widget","unitPriceCents":1250,"quantity":1}],"shippingAddress":{"line1":"","city":"London","postalCode":"N1 1AA","country":"GB"},"paymentMethodId":"'"$GOOD_METHOD"'"}'
check "status" "400" "$STATUS"
check "stub calls" "" "$(calls_shape)"

echo
echo "9. Only the authorized order exists..."
check "order rows" "1" "$(psql_q "SELECT count(*) FROM \"order\"")"

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
