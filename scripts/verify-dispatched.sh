#!/usr/bin/env bash
# Live verification for track dispatch-confirmed_20261028 (US-17.4).
# Authorized order -> fulfilment simulate dispatch -> order status dispatched.
# A forced fulfilment failure must leave the order authorized (US-18.3 not
# built yet). Cleans up its test orders/tasks.
set -euo pipefail

NAMESPACE="${NAMESPACE:-gluon-local}"
ORDER_URL="${ORDER_URL:-http://localhost:18080}"
PAYMENT_URL="${PAYMENT_URL:-http://localhost:18081}"
INVENTORY_URL="${INVENTORY_URL:-http://localhost:18082}"
FULFILMENT_URL="${FULFILMENT_URL:-http://localhost:18085}"
PIDS=()
TMP_DIR="$(mktemp -d)"
cleanup() {
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

start_pf() { kubectl -n "$NAMESPACE" port-forward "svc/$1" "$2:8080" >/dev/null 2>&1 & PIDS+=("$!"); }
wait_ready() {
  for _ in $(seq 1 30); do curl -sf "$1/health/ready" >/dev/null 2>&1 && return 0; sleep 1; done
  echo "FAIL: $2 never became ready" >&2; exit 1
}
post_json() { curl -sS -o "$3" -w '%{http_code}' -X POST "$1" -H 'Content-Type: application/json' -d "$2"; }
json_field() { python3 -c "import json,sys; print(json.load(sys.stdin)$1)" < "$2"; }

echo "== US-17.4 live verification =="
start_pf order-service 18080
start_pf payment-service 18081
start_pf inventory-service 18082
start_pf fulfilment-service 18085
wait_ready "$ORDER_URL" order-service
wait_ready "$PAYMENT_URL" payment-service
wait_ready "$INVENTORY_URL" inventory-service
wait_ready "$FULFILMENT_URL" fulfilment-service

ADDRESS='{"line1":"1 Verification Lane","city":"London","postalCode":"EC1A 1BB","country":"GB"}'
CUSTOMER="$(python3 -c 'import uuid; print(uuid.uuid4())')"
SKU="$(curl -sf "$INVENTORY_URL/inventorys" | python3 -c '
import json,sys
rows=json.load(sys.stdin); stocked=[r for r in rows if r["quantityAvailable"]>0]
if not stocked: sys.exit("no stocked sku")
print(stocked[0]["sku"])')"

make_order() {
  local out="$1"
  local status
  status="$(post_json "$PAYMENT_URL/payments/methods" \
    "{\"customerId\":\"$CUSTOMER\",\"cardNumber\":\"4242424242424242\",\"expiry\":\"12/30\",\"billingAddress\":$ADDRESS}" "$TMP_DIR/pm.json")"
  [ "$status" = "201" ] || { echo "FAIL: payment method -> $status" >&2; exit 1; }
  local pm; pm="$(json_field '["id"]' "$TMP_DIR/pm.json")"
  status="$(post_json "$ORDER_URL/orders" \
    "{\"customerId\":\"$CUSTOMER\",\"items\":[{\"sku\":\"$SKU\",\"productName\":\"Verify Watch\",\"unitPriceCents\":100,\"quantity\":1}],\"shippingAddress\":$ADDRESS,\"paymentMethodId\":\"$pm\"}" "$out")"
  [ "$status" = "201" ] || { echo "FAIL: order -> $status" >&2; cat "$out" >&2; exit 1; }
  json_field '["id"]' "$out"
}

wait_task() {
  local order_id="$1" state="$2"
  for _ in $(seq 1 30); do
    curl -sf "$FULFILMENT_URL/fulfilment-tasks?state=$state" | grep -q "$order_id" && return 0
    sleep 1
  done
  echo "FAIL: no $state fulfilment task for $order_id" >&2; exit 1
}

order_status() { curl -sf "$ORDER_URL/orders/$1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])'; }

echo "-- order 1 -> dispatched --"
ORDER1="$(make_order "$TMP_DIR/order1.json")"
wait_task "$ORDER1" PENDING
for _ in 1 2 3; do
  STATUS="$(post_json "$FULFILMENT_URL/fulfilment-tasks/$ORDER1/simulate-next" '{}' "$TMP_DIR/sim.json")"
  [ "$STATUS" = "200" ] || { echo "FAIL: simulate -> $STATUS" >&2; exit 1; }
done
for _ in $(seq 1 20); do
  [ "$(order_status "$ORDER1")" = "dispatched" ] && break
  sleep 1
done
STATE="$(order_status "$ORDER1")"
[ "$STATE" = "dispatched" ] || { echo "FAIL: expected dispatched, got $STATE" >&2; exit 1; }
echo "   order $ORDER1 -> dispatched"

echo "-- order 2 forced failure leaves it authorized --"
ORDER2="$(make_order "$TMP_DIR/order2.json")"
wait_task "$ORDER2" PENDING
STATUS="$(post_json "$FULFILMENT_URL/fulfilment-tasks/$ORDER2/simulate-next" \
  '{"forceFailure":true,"reason":"Simulated failure"}' "$TMP_DIR/fail.json")"
[ "$STATUS" = "200" ] || { echo "FAIL: forced failure -> $STATUS" >&2; exit 1; }
sleep 4
STATE="$(order_status "$ORDER2")"
[ "$STATE" = "authorized" ] || { echo "FAIL: expected authorized (US-18.3 not built), got $STATE" >&2; exit 1; }
echo "   order $ORDER2 still authorized"

echo "-- cleaning up --"
for id in "$ORDER1" "$ORDER2"; do
  curl -sf -X POST "$INVENTORY_URL/inventorys/order-reservations/$id/release" >/dev/null || true
  curl -sf -X DELETE "$ORDER_URL/orders/$id" >/dev/null || true
done
kubectl exec -n "$NAMESPACE" deploy/fulfilment-postgres -- psql -U fulfilment -d fulfilment -q -c \
  "delete from outbox_events where aggregate_id in ('$ORDER1','$ORDER2'); delete from fulfilment_task where order_id in ('$ORDER1','$ORDER2')" >/dev/null

echo "All US-17.4 live checks passed."
