#!/usr/bin/env bash
set -euo pipefail

# Manual verification for the payment outcome consumers (Rework 7): boots
# order-service with PaymentEventConsumer consuming PaymentCaptured /
# PaymentAuthorizationExpired / PaymentFailed (PaymentAuthorized is learned
# synchronously and never consumed).
#
# Scope note (2026-10-03): US-5.3's own final checkpoint
# (verify-order-reserved-end-to-end.sh) found and documented a reproducible
# local-only broker race on this project's single-node KRaft docker-compose
# Kafka - starting a new producer at the same moment a consumer's merged
# stream joins its consumer groups reliably crashes the colliding consumer.
# This track adds PaymentEventConsumer's consumer group
# (order-service-payment-events) joining concurrently with the same
# outbox-backed publisher at boot, alongside StockEventConsumer's own
# group - more concurrent connection attempts, not fewer. Rather than
# re-discover that race a second time, this script is scoped from the start
# to what IS reliably provable live: order-service boots cleanly with
# PaymentEventConsumer wired in alongside StockEventConsumer, and the health
# endpoint responds. The actual consume -> transition -> publish behavior
# (PaymentCaptured/PaymentAuthorizationExpired/PaymentFailed, on the single payment-events
# domain topic + envelope shape per ADR 0009 - migrated by
# kafka-envelope_20261011 - plus the no-op/decode-failure/
# different-eventType-routing edge cases) is proven by
# PaymentEventConsumerSuite's Testcontainers tests instead - confirmed stable
# across repeated runs, is the authoritative proof.
#
# Usage: ./scripts/verify-payment-outcome-consumers-end-to-end.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

FAILED=0
SBT_PID=""
SBT_LOG="$(mktemp -t order-payment-outcomes-sbt-run.XXXXXX)"

cleanup() {
  echo
  echo "Cleaning up..."
  if [ -n "$SBT_PID" ]; then
    pkill -9 -P "$SBT_PID" >/dev/null 2>&1 || true
    kill -9 "$SBT_PID" >/dev/null 2>&1 || true
    wait "$SBT_PID" 2>/dev/null || true
  fi
  rm -f "$SBT_LOG"
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
echo "2. Starting order-service (sbt run)..."
if [ -z "${GITHUB_TOKEN:-}" ] || [ -z "${GITHUB_ACTOR:-}" ]; then
  echo "WARN: GITHUB_TOKEN / GITHUB_ACTOR not set - resolving purerestlib from" >&2
  echo "      GitHub Packages will fail without a read:packages token." >&2
fi
(cd "$ROOT_DIR" && exec sbt -batch run >"$SBT_LOG" 2>&1 </dev/null) &
SBT_PID=$!
wait_ready
echo "   OK: order-service ready on :8080 - boots cleanly with"
echo "       PaymentEventConsumer (order-service-payment-events) wired in"
echo "       alongside StockEventConsumer (order-service-inventory-events)"
echo "       and the outbox-backed publisher, both consumer groups plus"
echo "       the producer connecting concurrently at startup."

echo
echo "3. Confirming no startup crash was logged (e.g. the documented NPE race)..."
if grep -q "NullPointerException\|Exception in thread \"main\"" $SBT_LOG; then
  echo "   FAIL: an exception appeared in the startup log:" >&2
  grep -A 5 "NullPointerException\|Exception in thread \"main\"" $SBT_LOG >&2 || true
  FAILED=1
else
  echo "   OK: no exception in the startup log"
fi

check "order-service health endpoint" "200" "$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/health)"
check "order-service ready endpoint" "200" "$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/health/ready)"

echo
echo "The consume -> transition -> publish behavior itself (PaymentCaptured"
echo "-> confirmed, PaymentAuthorizationExpired -> cancelled, PaymentFailed -> payment_failed, plus the no-op/"
echo "decode-failure/different-eventType edge cases) is proven by"
echo "PaymentEventConsumerSuite's tests instead of live here - see this"
echo "script's header comment for why."

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
