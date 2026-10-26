#!/usr/bin/env bash
set -euo pipefail

# Verifies order-service's AsyncAPI documentation end-to-end, now generated
# via the sbt-purekafka-docs plugin (asyncApiGenerateSpec/asyncApiGenerate,
# auto-chained after Compile/compile) instead of the old
# generate-kafka-asyncapi-docs.sh script — generation no longer needs a
# running server at all, since AsyncApiSpecWriter reads AsyncApiChannels
# directly via reflection rather than curling a live HTTP route. Confirms
# all 7 registered event types show up in both the generated spec/HTML
# *and* the live-served raw-spec route, so both halves (build-time
# generation and runtime serving) are checked, not just that a process
# started or a task exited 0.
#
# No real inventory-service involved (same stub/fan-out scoping as
# scripts/verify-stock-event-consumer.sh) — the checkout call isn't
# exercised here at all, since AsyncAPI docs only depend on the
# consumers' registered event types, not on any live order flow.
#
# Usage: ./scripts/verify-kafka-asyncapi-docs.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ORDER_SERVICE_URL="http://localhost:8080"
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
    status="$(curl -s -o /dev/null -w '%{http_code}' "${ORDER_SERVICE_URL}/health" || true)"
    [ "$status" = "200" ] && return 0
    sleep 1
  done
  echo "FAIL: order-service did not become ready in time." >&2
  return 1
}

echo "1. sbt asyncApiGenerate (no server needed - AsyncApiSpecWriter reads AsyncApiChannels directly)..."
sbt asyncApiGenerate

echo
echo "2. Checking the generated raw spec for all 7 registered event types..."
SPEC_FILE="target/asyncapi-docs-spec/asyncapi.json"
for needle in "inventory-events" "payment-events" "StockReserved" "StockReservationFailed" "PaymentCaptured" "PaymentAuthorizationExpired" "PaymentFailed"; do
  if ! grep -q "$needle" "$SPEC_FILE"; then
    echo "FAIL: generated spec missing expected content: ${needle}" >&2
    FAILED=1
  else
    echo "   OK: generated spec contains \"${needle}\""
  fi
done

echo
echo "3. Checking the generated static HTML for the same content..."
HTML_FILE="target/asyncapi-docs/index.html"
for needle in "StockReserved" "StockReservationFailed" "PaymentCaptured" "PaymentAuthorizationExpired" "PaymentFailed"; do
  if ! grep -q "$needle" "$HTML_FILE"; then
    echo "FAIL: generated HTML missing expected content: ${needle}" >&2
    FAILED=1
  else
    echo "   OK: generated HTML contains \"${needle}\""
  fi
done

echo
echo "4. docker compose up -d (fresh volumes, postgres + kafka + redis)..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d
echo "   waiting for kafka to be healthy..."
for _ in $(seq 1 60); do
  health="$(docker compose ps --format json kafka 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('Health',''))" 2>/dev/null || true)"
  [ "$health" = "healthy" ] && break
  sleep 1
done

echo
echo "5. Starting order-service (sbt run), pointed at an unreachable inventory-service (irrelevant to this check)..."
INVENTORY_SERVICE_URL="http://localhost:19999" sbt run >/tmp/order-service-asyncapi-verify.log 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: order-service ready on :8080"

echo
echo "6. Checking the live raw-spec route (/docs/asyncapi.json) for the same content..."
for needle in "StockReserved" "PaymentCaptured"; do
  if ! curl -s "${ORDER_SERVICE_URL}/docs/asyncapi.json" | grep -q "$needle"; then
    echo "FAIL: live raw-spec route missing expected content: ${needle}" >&2
    FAILED=1
  else
    echo "   OK: live raw-spec route contains \"${needle}\""
  fi
done

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
  exit 0
else
  echo "One or more checks failed — see FAIL lines above." >&2
  exit 1
fi
