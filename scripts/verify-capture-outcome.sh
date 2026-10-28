#!/usr/bin/env bash
# Live verification for track capture-outcome_20261028 (US-19.2 + US-19.3).
# 1) dispatched order, normal card -> order `confirmed`; duplicate
#    PaymentCaptured is harmless (one OrderStatusChanged row);
# 2) decline-at-capture card -> order `payment_failed`, an
#    order_reconciliation row (`capture_failed`), and its inventory
#    reservations are NOT released. Cleans up test data.
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
order_status() { curl -sf "$ORDER_URL/orders/$1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])'; }
stocked_sku() {
  curl -sf "$INVENTORY_URL/inventorys" | python3 -c '
import json,sys
rows=json.load(sys.stdin); stocked=[r for r in rows if r["quantityAvailable"]>0]
if not stocked: sys.exit("no stocked sku")
print(stocked[0]["sku"])'
}
reserved_for_sku() {
  curl -sf "$INVENTORY_URL/inventorys?sku=$1" | python3 -c '
import json,sys
rows=json.load(sys.stdin)
print(rows[0]["quantityReserved"] if rows else -1)'
}
reconciliation_reason() {
  kubectl exec -n "$NAMESPACE" deploy/order-postgres -- psql -U order -d order -tAc \
    "select coalesce((select reason from order_reconciliation where order_id = '$1'), '')" | tr -d '[:space:]'
}
confirmed_outbox_count() {
  kubectl exec -n "$NAMESPACE" deploy/order-postgres -- psql -U order -d order -tAc \
    "select count(*) from outbox_events where aggregate_id = '$1' and payload->>'eventType' = 'OrderStatusChanged' and payload->'payload'->>'status' = 'confirmed'" | tr -d '[:space:]'
}
wait_order_status() {
  for _ in $(seq 1 30); do
    [ "$(order_status "$1")" = "$2" ] && return 0
    sleep 1
  done
  echo "FAIL: order $1 never became $2 (is $(order_status "$1"))" >&2
  exit 1
}
wait_reserved() {
  for _ in $(seq 1 30); do
    [ "$(reserved_for_sku "$1")" = "$2" ] && return 0
    sleep 1
  done
  echo "FAIL: reserved for $1 never became $2 (is $(reserved_for_sku "$1"))" >&2
  exit 1
}
wait_task() {
  for _ in $(seq 1 30); do
    curl -sf "$FULFILMENT_URL/fulfilment-tasks?state=PENDING" | grep -q "$1" && return 0
    sleep 1
  done
  echo "FAIL: no PENDING task for $1" >&2; exit 1
}
drive_to_dispatch() {
  wait_task "$1"
  for step in 1 2 3; do
    STATUS="$(post_json "$FULFILMENT_URL/fulfilment-tasks/$1/simulate-next" '{}' "$TMP_DIR/step.json")"
    [ "$STATUS" = "200" ] || { echo "FAIL: simulate-next step $step -> $STATUS" >&2; cat "$TMP_DIR/step.json" >&2; exit 1; }
  done
}
make_order() {
  local card="$1" sku="$2"
  STATUS="$(post_json "$PAYMENT_URL/payments/methods" \
    "{\"customerId\":\"$CUSTOMER\",\"cardNumber\":\"$card\",\"expiry\":\"12/30\",\"billingAddress\":$ADDRESS}" "$TMP_DIR/pm.json")"
  [ "$STATUS" = "201" ] || { echo "FAIL: payment method -> $STATUS" >&2; cat "$TMP_DIR/pm.json" >&2; exit 1; }
  local pm; pm="$(json_field '["id"]' "$TMP_DIR/pm.json")"
  STATUS="$(post_json "$ORDER_URL/orders" \
    "{\"customerId\":\"$CUSTOMER\",\"items\":[{\"sku\":\"$sku\",\"productName\":\"Verify Watch\",\"unitPriceCents\":100,\"quantity\":1}],\"shippingAddress\":$ADDRESS,\"paymentMethodId\":\"$pm\"}" "$TMP_DIR/order.json")"
  [ "$STATUS" = "201" ] || { echo "FAIL: order -> $STATUS" >&2; cat "$TMP_DIR/order.json" >&2; exit 1; }
  json_field '["id"]' "$TMP_DIR/order.json"
}
inject_payment_captured() {
  local order_id="$1"
  python3 -c "
import json,uuid
print(json.dumps({'eventId':str(uuid.uuid4()),'eventType':'PaymentCaptured','timestamp':'2026-10-28T12:00:00Z','source':'verify-script','correlationId':'$order_id','payload':{'orderId':'$order_id','amountCents':100,'timestamp':'2026-10-28T12:00:00Z'}}))" |
    kubectl exec -i -n "$NAMESPACE" deploy/kafka -- \
      /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
      --topic payment-events >/dev/null
}
cleanup_order() {
  local order_id="$1"
  curl -sf -X POST "$INVENTORY_URL/inventorys/order-reservations/$order_id/release" >/dev/null || true
  curl -sf -X DELETE "$ORDER_URL/orders/$order_id" >/dev/null || true
  kubectl exec -n "$NAMESPACE" deploy/order-postgres -- psql -U order -d order -q -c \
    "delete from outbox_events where aggregate_id = '$order_id'" >/dev/null
  kubectl exec -n "$NAMESPACE" deploy/fulfilment-postgres -- psql -U fulfilment -d fulfilment -q -c \
    "delete from outbox_events where aggregate_id = '$order_id'; delete from fulfilment_task where order_id = '$order_id'" >/dev/null
  kubectl exec -n "$NAMESPACE" deploy/payment-postgres -- psql -U payment -d payment -q -c \
    "delete from outbox_events where aggregate_id = '$order_id'; delete from payment where order_id = '$order_id'" >/dev/null
}

echo "== US-19.2/19.3 live verification =="
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

echo "-- capture confirms the order --"
ORDER_OK="$(make_order 4242424242424242 "$(stocked_sku)")"
drive_to_dispatch "$ORDER_OK"
wait_order_status "$ORDER_OK" confirmed
[ "$(reconciliation_reason "$ORDER_OK")" = "" ] || { echo "FAIL: success path set a reconciliation flag" >&2; exit 1; }
echo "   order $ORDER_OK confirmed"

echo "-- duplicate PaymentCaptured is harmless --"
inject_payment_captured "$ORDER_OK"
sleep 3
[ "$(order_status "$ORDER_OK")" = "confirmed" ] || { echo "FAIL: duplicate changed the status" >&2; exit 1; }
[ "$(confirmed_outbox_count "$ORDER_OK")" = "1" ] || { echo "FAIL: duplicate published another OrderStatusChanged" >&2; exit 1; }
echo "   still confirmed, one OrderStatusChanged row"

echo "-- capture failure flags reconciliation and does NOT release inventory --"
SKU_DECLINED="$(stocked_sku)"
BASELINE="$(reserved_for_sku "$SKU_DECLINED")"
ORDER_DECLINED="$(make_order 5555555555554444 "$SKU_DECLINED")"
EXPECTED_RESERVED=$((BASELINE + 1))
wait_reserved "$SKU_DECLINED" "$EXPECTED_RESERVED"
drive_to_dispatch "$ORDER_DECLINED"
wait_order_status "$ORDER_DECLINED" payment_failed
[ "$(reconciliation_reason "$ORDER_DECLINED")" = "capture_failed" ] || { echo "FAIL: missing capture_failed reconciliation flag" >&2; exit 1; }
sleep 3
[ "$(reserved_for_sku "$SKU_DECLINED")" = "$EXPECTED_RESERVED" ] || { echo "FAIL: inventory was auto-released" >&2; exit 1; }
echo "   order $ORDER_DECLINED payment_failed; reserved still $EXPECTED_RESERVED (no auto-release)"

echo "-- cleaning up --"
cleanup_order "$ORDER_OK"
cleanup_order "$ORDER_DECLINED"
kubectl exec -n "$NAMESPACE" deploy/payment-postgres -- psql -U payment -d payment -q -c \
  "delete from payment_methods where customer_id = '$CUSTOMER'" >/dev/null

echo "All US-19.2/19.3 live checks passed."
