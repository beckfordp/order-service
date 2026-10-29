#!/usr/bin/env bash
# Live verification for track dispatch-failed_20261028 (US-18.3).
# 1) authorized order forced to fail in fulfilment -> order dispatch_failed and
#    its reserved inventory returns to available;
# 2) a DispatchFailed for an already-dispatched order is skipped.
# Cleans up test data.
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

echo "== US-18.3 live verification =="
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
reserved_for_sku() {
  curl -sf "$INVENTORY_URL/inventorys?sku=$SKU" | python3 -c '
import json,sys
rows=json.load(sys.stdin)
print(rows[0]["quantityReserved"] if rows else -1)'
}
BASELINE="$(reserved_for_sku)"

make_order() {
  local out="$1" status
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
  for _ in $(seq 1 30); do
    curl -sf "$FULFILMENT_URL/fulfilment-tasks?state=PENDING" | grep -q "$1" && return 0
    sleep 1
  done
  echo "FAIL: no PENDING task for $1" >&2; exit 1
}
order_status() { curl -sf "$ORDER_URL/orders/$1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])'; }

echo "-- forced failure releases and sets dispatch_failed --"
ORDER1="$(make_order "$TMP_DIR/order1.json")"
wait_task "$ORDER1"
STATUS="$(post_json "$FULFILMENT_URL/fulfilment-tasks/$ORDER1/simulate-next" \
  '{"forceFailure":true,"reason":"Simulated warehouse failure"}' "$TMP_DIR/fail.json")"
[ "$STATUS" = "200" ] || { echo "FAIL: forced failure -> $STATUS" >&2; exit 1; }
for _ in $(seq 1 20); do
  [ "$(order_status "$ORDER1")" = "dispatch_failed" ] && break
  sleep 1
done
STATE="$(order_status "$ORDER1")"
[ "$STATE" = "dispatch_failed" ] || { echo "FAIL: expected dispatch_failed, got $STATE" >&2; exit 1; }
for _ in $(seq 1 20); do
  [ "$(reserved_for_sku)" = "$BASELINE" ] && break
  sleep 1
done
RESERVED="$(reserved_for_sku)"
[ "$RESERVED" = "$BASELINE" ] || { echo "FAIL: reserved $RESERVED != baseline $BASELINE" >&2; exit 1; }
echo "   order $ORDER1 dispatch_failed; reserved back to $BASELINE"

echo "-- DispatchFailed for an already-dispatched order is skipped --"
ORDER2="$(make_order "$TMP_DIR/order2.json")"
wait_task "$ORDER2"
for _ in 1 2 3; do
  post_json "$FULFILMENT_URL/fulfilment-tasks/$ORDER2/simulate-next" '{}' "$TMP_DIR/sim.json" >/dev/null
done
for _ in $(seq 1 20); do
  { [ "$(order_status "$ORDER2")" = "dispatched" ] ||
    [ "$(order_status "$ORDER2")" = "confirmed" ]; } && break
  sleep 1
done
{ [ "$(order_status "$ORDER2")" = "dispatched" ] ||
  [ "$(order_status "$ORDER2")" = "confirmed" ]; } || {
  echo "FAIL: order2 not dispatched/confirmed" >&2
  exit 1
}
EVENT="$(python3 -c "
import json,uuid
print(json.dumps({'eventId':str(uuid.uuid4()),'eventType':'DispatchFailed','timestamp':'2026-10-28T12:00:00Z','source':'verify-script','correlationId':'$ORDER2','payload':{'orderId':'$ORDER2','reason':'late failure'}}))")"
printf '%s\n' "$EVENT" | kubectl exec -i -n "$NAMESPACE" deploy/kafka -- \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic fulfilment-events >/dev/null
sleep 4
STATE="$(order_status "$ORDER2")"
{ [ "$STATE" = "dispatched" ] || [ "$STATE" = "confirmed" ]; } || {
  echo "FAIL: dispatched order moved to $STATE" >&2
  exit 1
}
echo "   order $ORDER2 stayed $STATE (confirmed is capture-on-dispatch)"

echo "-- cleaning up --"
for id in "$ORDER1" "$ORDER2"; do
  curl -sf -X POST "$INVENTORY_URL/inventorys/order-reservations/$id/release" >/dev/null || true
  curl -sf -X DELETE "$ORDER_URL/orders/$id" >/dev/null || true
done
kubectl exec -n "$NAMESPACE" deploy/fulfilment-postgres -- psql -U fulfilment -d fulfilment -q -c \
  "delete from outbox_events where aggregate_id in ('$ORDER1','$ORDER2'); delete from fulfilment_task where order_id in ('$ORDER1','$ORDER2')" >/dev/null
# Release after the dispatched order's deletion so the baseline holds.
curl -sf -X POST "$INVENTORY_URL/inventorys/order-reservations/$ORDER2/release" >/dev/null || true

echo "All US-18.3 live checks passed."
