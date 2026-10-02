#!/usr/bin/env bash
set -euo pipefail

# Verifies US-5.2 (track stock-event-consumer_20261002) end-to-end: brings up
# Postgres + Kafka + a real sbt run instance, drives a real checkout, then
# publishes a real Kafka message (via the broker's own console producer, not
# a stub) to confirm the live consumer wired into Main.scala actually flips
# order status - not just that the Testcontainers-backed test suite passes.
# No real inventory-service involved (per PLAN.md's stub/fan-out guidance -
# order-service's own checks never depend on a live inventory-service); the
# checkout call itself points at an unreachable port and is expected to end
# up reservation_failed synchronously, which is irrelevant here since this
# script drives the order to pending via a direct DB update before publishing
# the async event, isolating the one thing this phase adds: the consumer.
#
# Usage: ./scripts/verify-stock-event-consumer.sh

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

publish() {
  local topic="$1" key="$2" json="$3"
  # --property parse.key=true: without it, kafka-console-producer.sh sends a
  # NULL key, which crashes fs2-kafka's (non-null-safe) String key
  # deserializer with the exact NPE this script originally hit - a bug in
  # this script, not in StockEventConsumer (its own tests always pass an
  # explicit key).
  echo "${key}:${json}" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic "$topic" \
    --property parse.key=true \
    --property key.separator=: >/dev/null 2>&1
}

poll_for_status() {
  local order_id="$1" expected="$2"
  for _ in $(seq 1 30); do
    status="$(curl -s "http://localhost:8080/orders/${order_id}" | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])" 2>/dev/null || true)"
    [ "$status" = "$expected" ] && { echo "$status"; return 0; }
    sleep 1
  done
  echo "${status:-<timeout>}"
}

echo "1. docker compose up -d (fresh volumes, postgres + kafka)..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d
echo "   OK: postgres + kafka starting"
echo "   waiting for kafka to be healthy..."
for _ in $(seq 1 60); do
  health="$(docker compose ps --format json kafka 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('Health',''))" 2>/dev/null || true)"
  [ "$health" = "healthy" ] && break
  sleep 1
done

echo
echo "1.5. Pre-creating both topics (avoids any startup-time surprise from auto-creation-on-subscribe)..."
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic inventory.stock-reserved --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 >/dev/null
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic inventory.stock-reservation-failed --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 >/dev/null
echo "   OK: topics created"
echo "   giving the single-node broker's group coordinator a moment to settle..."
sleep 10

echo
echo "2. Starting order-service (sbt run), pointed at an unreachable inventory-service (irrelevant to this check)..."
if [ -z "${GITHUB_TOKEN:-}" ] || [ -z "${GITHUB_ACTOR:-}" ]; then
  echo "WARN: GITHUB_TOKEN / GITHUB_ACTOR not set - resolving purerestlib from" >&2
  echo "      GitHub Packages will fail without a read:packages token." >&2
fi
INVENTORY_SERVICE_URL="http://localhost:19999" sbt run >/tmp/order-service-verify.log 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: order-service ready on :8080"

echo
echo "3. Checkout (status will end up reservation_failed synchronously - expected, since inventory-service is unreachable)..."
order1_response="$(curl -s -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust-123","items":[{"sku":"sku-1","productName":"Widget","unitPriceCents":999,"quantity":2}]}')"
order1_id="$(echo "$order1_response" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")"
order1_item_id="$(echo "$order1_response" | python3 -c "import json,sys; print(json.load(sys.stdin)['items'][0]['id'])")"
order2_response="$(curl -s -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust-456","items":[{"sku":"sku-2","productName":"Gadget","unitPriceCents":500,"quantity":1}]}')"
order2_id="$(echo "$order2_response" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")"
order2_item_id="$(echo "$order2_response" | python3 -c "import json,sys; print(json.load(sys.stdin)['items'][0]['id'])")"
echo "   order1=${order1_id} item=${order1_item_id}, order2=${order2_id} item=${order2_item_id}"

echo
echo "4. Resetting both orders to pending directly via Postgres (isolating the consumer from the synchronous reserve outcome)..."
docker compose exec -T postgres psql -U order -d order -c \
  "update \"order\" set status = 'pending' where id in ('${order1_id}', '${order2_id}')" >/dev/null
check "both orders reset to pending" "pending" "$(poll_for_status "$order1_id" pending)"

echo
echo "5. Publishing a real inventory.stock-reserved message for order1's item via the broker's own console producer..."
publish "inventory.stock-reserved" "sku-1" "{\"orderItemId\":\"${order1_item_id}\",\"sku\":\"sku-1\",\"quantity\":2,\"timestamp\":\"2026-01-01T00:00:00Z\"}"
check "order1 status flips to reserved" "reserved" "$(poll_for_status "$order1_id" reserved)"

echo
echo "6. Publishing a real inventory.stock-reservation-failed message for order2's item..."
publish "inventory.stock-reservation-failed" "sku-2" "{\"orderItemId\":\"${order2_item_id}\",\"sku\":\"sku-2\",\"quantity\":1,\"timestamp\":\"2026-01-01T00:00:00Z\"}"
check "order2 status flips to reservation_failed" "reservation_failed" "$(poll_for_status "$order2_id" reservation_failed)"

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
