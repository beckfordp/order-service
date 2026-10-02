#!/usr/bin/env bash
set -euo pipefail

# Verifies OrderEventPublisher's wire contract - originally written for
# Phase 2 of US-5.3 (order.reserved), extended for Phase 1 of US-5.4
# (order.status-changed). OrderEventPublisherSuite's Testcontainers tests
# already cover the publisher itself (happy path + bounded-retry exhaustion)
# against a real broker, so the one thing worth confirming independently,
# against this repo's own docker-compose Kafka rather than an ephemeral
# Testcontainers one, is the wire contract: the topic names and JSON shapes
# OrderEventPublisher actually produces match what
# gluon/docs/system-design.md pins - the thing payment-service's (US-6.1)
# and notification-service's (US-7.1) consumers have to agree with by
# reading that doc, not this repo's source.
#
# Usage: ./scripts/verify-order-event-publisher.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

FAILED=0

cleanup() {
  echo
  echo "Cleaning up..."
  docker compose down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

check() {
  local label="$1" expected="$2" actual="$3"
  if [ "$actual" = "$expected" ]; then
    echo "   OK: ${label} (got ${actual})"
  else
    echo "   FAIL: ${label} - expected ${expected}, got ${actual}" >&2
    FAILED=1
  fi
}

echo "1. docker compose up -d kafka (fresh volumes)..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d kafka
echo "   OK: kafka starting"

for _ in $(seq 1 60); do
  if docker compose exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

roundtrip_check() {
  local label="$1" topic="$2" payload="$3"
  local consumed
  echo "$payload" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic "$topic" >/dev/null
  consumed="$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "$topic" \
    --from-beginning --max-messages 1 --timeout-ms 10000 2>/dev/null || true)"
  # Strip trailing \r that docker compose exec -T can leave on the line.
  consumed="$(echo "$consumed" | tr -d '\r')"
  check "${label} round-tripped payload matches" "$payload" "$consumed"
}

echo
echo "2. Confirming OrderEventPublisher's topic constants match what's pinned..."
RESERVED_TOPIC_CONST="$(sed -n 's/.*val reservedTopic: String = "\([^"]*\)".*/\1/p' src/main/scala/orderservice/OrderEventPublisher.scala)"
STATUS_CHANGED_TOPIC_CONST="$(sed -n 's/.*val statusChangedTopic: String = "\([^"]*\)".*/\1/p' src/main/scala/orderservice/OrderEventPublisher.scala)"
check "reservedTopic constant is order.reserved" "order.reserved" "$RESERVED_TOPIC_CONST"
check "statusChangedTopic constant is order.status-changed" "order.status-changed" "$STATUS_CHANGED_TOPIC_CONST"

echo
echo "3. Producing the exact JSON shapes OrderReservedEvent/OrderStatusChangedEvent's"
echo "   codecs emit, by hand, onto the real docker-compose Kafka (not Testcontainers),"
echo "   and reading them back..."

roundtrip_check "order.reserved" "$RESERVED_TOPIC_CONST" \
  '{"orderId":"order-verify-1","customerId":"cust-verify-1","totalCents":4242,"timestamp":"2026-01-01T00:00:00Z"}'
roundtrip_check "order.status-changed" "$STATUS_CHANGED_TOPIC_CONST" \
  '{"orderId":"order-verify-1","customerId":"cust-verify-1","status":"reservation_failed","timestamp":"2026-01-01T00:00:00Z"}'

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
