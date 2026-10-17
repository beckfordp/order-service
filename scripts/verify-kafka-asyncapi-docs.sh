#!/usr/bin/env bash
set -euo pipefail

# Verifies order-service's AsyncAPI documentation end-to-end
# (asyncapi-docs_20261017): brings up docker-compose (postgres + kafka +
# redis) and a real `sbt run` instance, runs
# scripts/generate-kafka-asyncapi-docs.sh to fetch the running spec and
# render static HTML, then confirms all 4 registered event types actually
# show up in both the raw spec and the served HTML — not just that the
# process started or the generator exited 0.
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

echo "1. docker compose up -d (fresh volumes, postgres + kafka + redis)..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d
echo "   waiting for kafka to be healthy..."
for _ in $(seq 1 60); do
  health="$(docker compose ps --format json kafka 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('Health',''))" 2>/dev/null || true)"
  [ "$health" = "healthy" ] && break
  sleep 1
done

echo
echo "2. Starting order-service (sbt run), pointed at an unreachable inventory-service (irrelevant to this check)..."
INVENTORY_SERVICE_URL="http://localhost:19999" sbt run >/tmp/order-service-asyncapi-verify.log 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: order-service ready on :8080"

echo
echo "3. Running scripts/generate-kafka-asyncapi-docs.sh..."
ORDER_SERVICE_URL="$ORDER_SERVICE_URL" ./scripts/generate-kafka-asyncapi-docs.sh

echo
echo "4. Checking the raw spec for all 4 registered event types..."
SPEC_FILE="target/asyncapi-docs-spec/asyncapi.json"
for needle in "inventory-events" "payment-events" "StockReserved" "StockReservationFailed" "PaymentSettled" "PaymentFailed"; do
  if ! grep -q "$needle" "$SPEC_FILE"; then
    echo "FAIL: raw spec missing expected content: ${needle}" >&2
    FAILED=1
  else
    echo "   OK: raw spec contains \"${needle}\""
  fi
done

echo
echo "5. Checking the served static HTML for the same content..."
html_status="$(curl -s -o /tmp/order-service-asyncapi.html -w '%{http_code}' "${ORDER_SERVICE_URL}/docs/asyncapi/index.html")"
if [ "$html_status" != "200" ]; then
  echo "FAIL: expected HTTP 200 serving the generated HTML, got ${html_status}" >&2
  FAILED=1
else
  for needle in "StockReserved" "PaymentSettled"; do
    if ! grep -q "$needle" /tmp/order-service-asyncapi.html; then
      echo "FAIL: served HTML missing expected content: ${needle}" >&2
      FAILED=1
    else
      echo "   OK: served HTML contains \"${needle}\""
    fi
  done
fi
rm -f /tmp/order-service-asyncapi.html

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
  exit 0
else
  echo "One or more checks failed — see FAIL lines above." >&2
  exit 1
fi
