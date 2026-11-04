#!/usr/bin/env bash
set -euo pipefail

# Phase 3 verification for journey-telemetry_20261103 (gluon PERF-4): after
# real traffic (gluon bin/run-acceptance drives every journey), order-service's
# metrics port in gluon-local exports the runtime, pool and business series
# this track adds.
#
# Usage: ./scripts/verify-journey-telemetry.sh   (after bin/k8s-local-up + traffic)

NAMESPACE="gluon-local"
LOCAL_PORT="${LOCAL_PORT:-19091}"
EXPECTED=(jvm_memory_used_bytes jvm_thread_count cats_effect_runtime db_client_connection_count db_client_connection_wait_time_seconds_bucket checkout_outcome_total orders_placed_total orders_status_transitions_total orders_status_time_in_state_seconds_bucket purekafka_consumer_event_age_seconds_bucket)

kubectl -n "$NAMESPACE" port-forward "svc/order-service" "$LOCAL_PORT:9090" >/dev/null 2>&1 &
PF_PID=$!
trap 'kill "$PF_PID" >/dev/null 2>&1 || true' EXIT
body=""
for _ in $(seq 1 20); do
  body="$(curl -fs "http://localhost:$LOCAL_PORT/metrics" || true)"
  [ -n "$body" ] && break
  sleep 0.5
done
[ -n "$body" ] || { echo "FAIL: order-service /metrics unreachable" >&2; exit 1; }

failed=0
for metric in "${EXPECTED[@]}"; do
  if grep -q "^$metric" <<< "$body"; then
    echo "  PASS $metric"
  else
    echo "  FAIL $metric missing"
    failed=1
  fi
done
[ "$failed" = 0 ] || { echo "FAIL: journey telemetry series missing" >&2; exit 1; }
echo "PASS: order-service journey telemetry live"
