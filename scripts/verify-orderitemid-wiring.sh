#!/usr/bin/env bash
set -euo pipefail

# Verifies Phase 2 of US-5.2 (wire orderItemId into the synchronous reserve
# call): brings up Postgres + a real sbt run instance, pointed at a minimal
# Python HTTP stand-in for inventory-service (not a real inventory-service -
# per PLAN.md's stub/fan-out guidance, order-service's own checks never
# depend on a live inventory-service). Confirms the REAL outbound HTTP
# request order-service sends for each line item actually carries the
# correct orderItemId - the one thing the test suite's own stub Client[F]
# tests (InventoryClientSuite) can't catch, since they bypass real JSON
# request-body encoding end-to-end through a live checkout call.
#
# Usage: ./scripts/verify-orderitemid-wiring.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

FAILED=0
SBT_PID=""
STUB_PID=""
STUB_LOG="$(mktemp /tmp/inventory-stub-log.XXXXXX)"
STUB_SCRIPT="$(mktemp /tmp/inventory-stub.XXXXXX.py)"

cleanup() {
  echo
  echo "Cleaning up..."
  [ -n "$SBT_PID" ] && kill "$SBT_PID" >/dev/null 2>&1 || true
  [ -n "$STUB_PID" ] && kill "$STUB_PID" >/dev/null 2>&1 || true
  rm -f "$STUB_LOG" "$STUB_SCRIPT"
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

cat > "$STUB_SCRIPT" <<'PYEOF'
import http.server
import sys

log_path, port = sys.argv[1], int(sys.argv[2])

class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8")
        with open(log_path, "a") as f:
            f.write(body + "\n")
        self.send_response(200)
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
echo "   OK: postgres starting"

echo
echo "2. Starting a minimal Python stand-in for inventory-service on :8081 (not a real inventory-service - captures requests only)..."
python3 "$STUB_SCRIPT" "$STUB_LOG" 8081 &
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
echo "4. Checkout with two line items..."
create_response="$(curl -s -w '\n%{http_code}' -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust-123","items":[{"sku":"sku-1","productName":"Widget","unitPriceCents":999,"quantity":2},{"sku":"sku-2","productName":"Gadget","unitPriceCents":500,"quantity":3}]}')"
create_status="$(echo "$create_response" | tail -1)"
create_body="$(echo "$create_response" | sed '$d')"
check "POST /orders returns 201" "201" "$create_status"
order_status="$(echo "$create_body" | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")"
check "order status is pending (both items reserved)" "pending" "$order_status"

echo
echo "5. Confirm the stub received two reserve requests, each with the real order_items.id as orderItemId..."
item1_id="$(echo "$create_body" | python3 -c "import json,sys; d=json.load(sys.stdin); print(next(i['id'] for i in d['items'] if i['sku']=='sku-1'))")"
item2_id="$(echo "$create_body" | python3 -c "import json,sys; d=json.load(sys.stdin); print(next(i['id'] for i in d['items'] if i['sku']=='sku-2'))")"
request_count="$(wc -l < "$STUB_LOG" | tr -d ' ')"
check "stub received exactly 2 reserve requests" "2" "$request_count"

check_request_for_item() {
  local sku="$1" quantity="$2" item_id="$3"
  local matched
  matched="$(python3 -c "
import json
with open('$STUB_LOG') as f:
    for line in f:
        d = json.loads(line)
        if d.get('sku') == '$sku' and d.get('quantity') == $quantity and d.get('orderItemId') == '$item_id':
            print('yes')
            break
    else:
        print('no')
")"
  check "a reserve request for ${sku} carries orderItemId=${item_id}" "yes" "$matched"
}
check_request_for_item "sku-1" 2 "$item1_id"
check_request_for_item "sku-2" 3 "$item2_id"

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
