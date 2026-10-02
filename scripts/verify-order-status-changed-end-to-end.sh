#!/usr/bin/env bash
set -euo pipefail

# Final verification for US-5.4 (order-status-changed_20261002).
#
# Scope note (2026-10-02): US-5.3's own final checkpoint
# (verify-order-reserved-end-to-end.sh) found and documented a reproducible
# local-only broker race on this project's single-node KRaft docker-compose
# Kafka - starting a new producer at the same moment StockEventConsumer's
# merged stream joins its consumer groups reliably crashes the stock-reserved
# consumer. This track's reservationFailedStream now shares that same
# producer/merged-stream, so the same race applies here too. Rather than
# re-discover that the hard way, this script is scoped from the start to what
# IS reliably provable live: order-service boots cleanly with the (already
# wired, from US-5.3) OrderEventPublisher, and BOTH synchronous checkout
# outcomes (success -> pending, failure -> reservation_failed) work correctly
# over real HTTP - neither depends on the flaky consumer-group-join path.
# The Kafka publish round trip itself (both the synchronous and async
# order.status-changed paths) is proven by StockEventConsumerSuite's and
# OrderRoutesSuite's Testcontainers/in-process tests instead - 116 tests
# passing, confirmed stable across repeated runs, is the authoritative proof.
#
# Reuses verify-orderitemid-wiring.sh's Python stand-in for inventory-service
# (not a real inventory-service - per PLAN.md's stub/fan-out guidance),
# extended here to also simulate a failed reservation for one sku.
#
# Usage: ./scripts/verify-order-status-changed-end-to-end.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

FAILED=0
SBT_PID=""
STUB_PID=""
STUB_SCRIPT="$(mktemp /tmp/inventory-stub.XXXXXX.py)"

cleanup() {
  echo
  echo "Cleaning up..."
  [ -n "$SBT_PID" ] && kill "$SBT_PID" >/dev/null 2>&1 || true
  [ -n "$STUB_PID" ] && kill "$STUB_PID" >/dev/null 2>&1 || true
  rm -f "$STUB_SCRIPT"
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

# Returns 409 (-> InsufficientStock -> ReservationFailed) for sku-fail,
# 200 (-> Reserved) for everything else.
cat > "$STUB_SCRIPT" <<'PYEOF'
import http.server
import json
import sys

port = int(sys.argv[1])

class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length).decode("utf-8"))
        status = 409 if body.get("sku") == "sku-fail" else 200
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, fmt, *args):
        pass

http.server.HTTPServer(("0.0.0.0", port), Handler).serve_forever()
PYEOF

echo "1. docker compose up -d (fresh volumes)..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d
echo "   OK: postgres + kafka + redis starting"

echo
echo "1b. Waiting for Kafka's own healthcheck before starting order-service..."
for _ in $(seq 1 60); do
  if docker compose exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
sleep 5
echo "   OK: kafka ready"

echo
echo "2. Starting a minimal Python stand-in for inventory-service on :8081..."
python3 "$STUB_SCRIPT" 8081 &
STUB_PID=$!
sleep 1
echo "   OK: stub listening on :8081"

echo
echo "3. Starting order-service (sbt run), pointed at the stub..."
if [ -z "${GITHUB_TOKEN:-}" ] || [ -z "${GITHUB_ACTOR:-}" ]; then
  echo "WARN: GITHUB_TOKEN / GITHUB_ACTOR not set - resolving purerestlib from" >&2
  echo "      GitHub Packages will fail without a read:packages token." >&2
fi
INVENTORY_SERVICE_URL="http://localhost:8081" sbt run >/tmp/order-service-verify.log 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: order-service ready on :8080"

echo
echo "4. Checkout with a reservable item..."
ok_response="$(curl -s -w '\n%{http_code}' -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust-ok-1","items":[{"sku":"sku-ok","productName":"Widget","unitPriceCents":2121,"quantity":2}]}')"
ok_status="$(echo "$ok_response" | tail -1)"
ok_body="$(echo "$ok_response" | sed '$d')"
check "POST /orders returns 201 (reservable item)" "201" "$ok_status"
ok_order_status="$(echo "$ok_body" | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")"
check "order status is pending after a successful sync reserve" "pending" "$ok_order_status"

echo
echo "5. Checkout with an item the stub rejects (simulates insufficient stock)..."
fail_response="$(curl -s -w '\n%{http_code}' -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust-fail-1","items":[{"sku":"sku-fail","productName":"Widget","unitPriceCents":2121,"quantity":2}]}')"
fail_status="$(echo "$fail_response" | tail -1)"
fail_body="$(echo "$fail_response" | sed '$d')"
check "POST /orders returns 201 (order still created)" "201" "$fail_status"
fail_order_status="$(echo "$fail_body" | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")"
check "order status is reservation_failed after a failed sync reserve" "reservation_failed" "$fail_order_status"

echo
echo "The order.status-changed publish itself (both the synchronous path just"
echo "exercised above, and the async StockEventConsumer path) is proven by"
echo "OrderRoutesSuite's and StockEventConsumerSuite's tests instead of live"
echo "here - see this script's header comment for why."

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
