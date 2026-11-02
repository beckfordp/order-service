#!/usr/bin/env bash
# Live verification for Checkout Journey 8 (US-22, ADR 0024).
#
# Starts throwaway Postgres, Kafka and Redis containers plus order-service
# itself, inserts authorized orders directly, then proves:
#   - no Authorization header = customer cancellation (audit + event actor);
#   - the configured operator token = operator cancellation (audit + event);
#   - a bad token = 401 and no state change;
#   - a repeat cancellation = 200, one event.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SERVICE_PORT="${SERVICE_PORT:-18081}"
METRICS_PORT="${METRICS_PORT:-19091}"
PG_PORT="${PG_PORT:-55433}"
KAFKA_PORT="${KAFKA_PORT:-59093}"
REDIS_PORT="${REDIS_PORT:-56379}"
PG_CONTAINER="order-verify-postgres-$$"
KAFKA_CONTAINER="order-verify-kafka-$$"
REDIS_CONTAINER="order-verify-redis-$$"
SERVICE_LOG="$(mktemp -t order-verify-log.XXXXXX)"
SBT_PID=""

cleanup() {
  if [ -n "$SBT_PID" ]; then
    pkill -P "$SBT_PID" 2>/dev/null || true
    kill "$SBT_PID" 2>/dev/null || true
    wait "$SBT_PID" 2>/dev/null || true
  fi
  docker rm -f "$PG_CONTAINER" "$KAFKA_CONTAINER" "$REDIS_CONTAINER" >/dev/null 2>&1 || true
  rm -f "$SERVICE_LOG"
}
trap cleanup EXIT

uuid() { python3 -c 'import uuid; print(uuid.uuid4())'; }

psql_query() {
  docker exec "$PG_CONTAINER" psql -U order -d order -tAc "$1"
}

echo "== US-22 operator cancellation live verification =="
docker run -d --rm --name "$PG_CONTAINER" \
  -p "$PG_PORT:5432" \
  -e POSTGRES_DB=order -e POSTGRES_USER=order -e POSTGRES_PASSWORD=order \
  postgres:16-alpine >/dev/null
docker run -d --rm --name "$REDIS_CONTAINER" \
  -p "$REDIS_PORT:6379" redis:7-alpine >/dev/null
docker run -d --rm --name "$KAFKA_CONTAINER" \
  -p "$KAFKA_PORT:29092" \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:29092 \
  -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://localhost:9092,EXTERNAL://localhost:$KAFKA_PORT" \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk apache/kafka:3.8.0 >/dev/null

for _ in $(seq 1 60); do
  docker exec "$PG_CONTAINER" pg_isready -U order >/dev/null 2>&1 && break
  sleep 1
done
docker exec "$PG_CONTAINER" pg_isready -U order >/dev/null || {
  echo "FAIL: Postgres never became ready" >&2; exit 1
}
for _ in $(seq 1 60); do
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server localhost:9092 >/dev/null 2>&1 && break
  sleep 1
done

echo "-- starting order-service --"
POSTGRES_HOST=localhost POSTGRES_PORT="$PG_PORT" \
  POSTGRES_DB=order POSTGRES_USER=order POSTGRES_PASSWORD=order \
  KAFKA_BOOTSTRAP_SERVERS="localhost:$KAFKA_PORT" \
  REDIS_URI="redis://localhost:$REDIS_PORT" \
  ORDER_SERVICE_PORT="$SERVICE_PORT" \
  ORDER_SERVICE_METRICS_PORT="$METRICS_PORT" \
  ORDER_OPERATOR_TOKEN="verify-operator-token" \
  ORDER_OPERATOR_ID="verify-operator" \
  sbt -batch run >"$SERVICE_LOG" 2>&1 </dev/null &
SBT_PID=$!

for _ in $(seq 1 120); do
  curl -sf "http://localhost:$SERVICE_PORT/health/ready" >/dev/null 2>&1 && break
  kill -0 "$SBT_PID" 2>/dev/null || {
    echo "FAIL: order-service exited during startup" >&2
    tail -40 "$SERVICE_LOG" >&2; exit 1
  }
  sleep 1
done
curl -sf "http://localhost:$SERVICE_PORT/health/ready" >/dev/null || {
  echo "FAIL: order-service never became ready" >&2
  tail -40 "$SERVICE_LOG" >&2; exit 1
}

insert_order() {
  local id="$1" customer="$2"
  psql_query "insert into \"order\" (id, customer_id, total_cents, status) values ('$id', '$customer', 4999, 'authorized')" >/dev/null
}

cancel() {
  local id="$1" header="${2:-}"
  if [ -n "$header" ]; then
    curl -sS -o /tmp/order-cancel-body.$$ -w '%{http_code}' -X PATCH \
      "http://localhost:$SERVICE_PORT/orders/$id" \
      -H 'Content-Type: application/json' -H "Authorization: $header" \
      -d '{"status":"cancelled"}'
  else
    curl -sS -o /tmp/order-cancel-body.$$ -w '%{http_code}' -X PATCH \
      "http://localhost:$SERVICE_PORT/orders/$id" \
      -H 'Content-Type: application/json' -d '{"status":"cancelled"}'
  fi
}

assert_audit() {
  local id="$1" expected="$2"
  local actual
  actual="$(psql_query "select actor_type || ':' || actor_id || ':' || reason from order_cancellation_audit where order_id = '$id'")"
  [ "$actual" = "$expected" ] || {
    echo "FAIL: audit for $id was '$actual', expected '$expected'" >&2; exit 1
  }
  echo "   OK: audit $actual"
}

assert_events() {
  local id="$1" expected="$2"
  local actual
  actual="$(psql_query "select count(*) from outbox_events where aggregate_id = '$id' and payload->>'eventType' = 'OrderCancelled'")"
  [ "$actual" = "$expected" ] || {
    echo "FAIL: $id has $actual OrderCancelled events, expected $expected" >&2; exit 1
  }
  echo "   OK: $actual OrderCancelled event(s)"
}

echo "-- customer cancellation --"
CUSTOMER_ORDER="$(uuid)"
insert_order "$CUSTOMER_ORDER" "verify-customer"
[ "$(cancel "$CUSTOMER_ORDER")" = "200" ] || { echo "FAIL: customer cancel" >&2; exit 1; }
assert_audit "$CUSTOMER_ORDER" "customer:verify-customer:customer"
assert_events "$CUSTOMER_ORDER" "1"

echo "-- repeat cancellation is idempotent --"
[ "$(cancel "$CUSTOMER_ORDER")" = "200" ] || { echo "FAIL: repeat cancel" >&2; exit 1; }
assert_events "$CUSTOMER_ORDER" "1"

echo "-- operator cancellation --"
OPERATOR_ORDER="$(uuid)"
insert_order "$OPERATOR_ORDER" "verify-customer-2"
[ "$(cancel "$OPERATOR_ORDER" "Bearer verify-operator-token")" = "200" ] || {
  echo "FAIL: operator cancel" >&2; exit 1
}
assert_audit "$OPERATOR_ORDER" "operator:verify-operator:operator"
assert_events "$OPERATOR_ORDER" "1"
ACTOR="$(psql_query "select ((payload::jsonb)->'payload'->'actor'->>'type') || ':' || ((payload::jsonb)->'payload'->'actor'->>'id') from outbox_events where aggregate_id = '$OPERATOR_ORDER' and ((payload::jsonb)->>'eventType') = 'OrderCancelled'")"
[ "$ACTOR" = "operator:verify-operator" ] || {
  echo "FAIL: event actor was '$ACTOR'" >&2; exit 1
}
echo "   OK: event actor $ACTOR"

echo "-- bad operator token --"
BAD_ORDER="$(uuid)"
insert_order "$BAD_ORDER" "verify-customer-3"
[ "$(cancel "$BAD_ORDER" "Bearer wrong-token")" = "401" ] || {
  echo "FAIL: bad token was not 401" >&2; exit 1
}
STATUS="$(psql_query "select status from \"order\" where id = '$BAD_ORDER'")"
[ "$STATUS" = "authorized" ] || { echo "FAIL: bad token changed status to $STATUS" >&2; exit 1; }
[ "$(psql_query "select count(*) from order_cancellation_audit where order_id = '$BAD_ORDER'")" = "0" ] || {
  echo "FAIL: bad token wrote an audit row" >&2; exit 1
}
echo "   OK: 401, no state change, no audit"

echo
echo "All US-22 live checks passed."
