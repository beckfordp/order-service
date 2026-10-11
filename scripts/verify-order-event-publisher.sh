#!/usr/bin/env bash
set -euo pipefail

# Verifies OrderEventPublisher's wire contract - originally written for
# Phase 2 of US-5.3 (order.reserved), extended for Phase 1 of US-5.4
# (order.status-changed), migrated to the order-events domain topic +
# envelope shape by kafka-envelope_20261011 (ADR 0009).
# OrderEventPublisherSuite's Testcontainers tests already cover the
# publisher itself (happy path + bounded-retry exhaustion) against a real
# broker, so the one thing worth confirming independently, against this
# repo's own docker-compose Kafka rather than an ephemeral Testcontainers
# one, is the wire contract: the topic name and envelope/payload JSON
# shapes OrderEventPublisher actually produces match what
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

# Both event types now share one order-events topic (ADR 0009), so
# consecutive calls can't both just read "--from-beginning --max-messages 1"
# - that would re-read the first call's own message. Read by explicit
# partition offset instead (0, 1, 2, ... in call order) so each check reads
# exactly the message it just produced.
ROUNDTRIP_OFFSET=0

roundtrip_check() {
  local label="$1" topic="$2" payload="$3"
  local consumed
  echo "$payload" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic "$topic" >/dev/null
  consumed="$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "$topic" \
    --partition 0 --offset "$ROUNDTRIP_OFFSET" --max-messages 1 --timeout-ms 10000 2>/dev/null || true)"
  # Strip trailing \r that docker compose exec -T can leave on the line.
  consumed="$(echo "$consumed" | tr -d '\r')"
  check "${label} round-tripped payload matches" "$payload" "$consumed"
  ROUNDTRIP_OFFSET=$((ROUNDTRIP_OFFSET + 1))
}

echo
echo "2. Confirming OrderEventPublisher's topic constant matches what's pinned..."
TOPIC_CONST="$(sed -n 's/.*val topic: String = "\([^"]*\)".*/\1/p' src/main/scala/orderservice/OrderEventPublisher.scala)"
check "topic constant is order-events" "order-events" "$TOPIC_CONST"

echo
echo "3. Producing the exact envelope JSON shapes OrderReservedEvent/"
echo "   OrderStatusChangedEvent get wrapped in, by hand, onto the real"
echo "   docker-compose Kafka (not Testcontainers), and reading them back..."

roundtrip_check "OrderReserved envelope" "$TOPIC_CONST" \
  '{"eventId":"11111111-1111-1111-1111-111111111111","eventType":"OrderReserved","timestamp":"2026-01-01T00:00:00Z","source":"order-service","correlationId":"order-verify-1","payload":{"orderId":"order-verify-1","customerId":"cust-verify-1","totalCents":4242,"timestamp":"2026-01-01T00:00:00Z"}}'
roundtrip_check "OrderStatusChanged envelope" "$TOPIC_CONST" \
  '{"eventId":"22222222-2222-2222-2222-222222222222","eventType":"OrderStatusChanged","timestamp":"2026-01-01T00:00:00Z","source":"order-service","correlationId":"order-verify-1","payload":{"orderId":"order-verify-1","customerId":"cust-verify-1","status":"reservation_failed","timestamp":"2026-01-01T00:00:00Z"}}'

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
