#!/usr/bin/env bash
set -euo pipefail

# Verifies order-service's schemaRegistryCheck against the real local Apicurio
# Registry, restored first from gluon's registry/ git store (gluon TD-18.5,
# ADR 0003's 2026-10-31 note).
#
# Proves: the check passes against the restored baseline; every artifact this
# service produces is registered and labelled producer=order-service; artifacts it
# only consumes are dry-run, never written (version counts unchanged); a
# second identical run adds no versions.
#
# Requires GITHUB_ACTOR/GITHUB_TOKEN (read:packages) to resolve
# sbt-purekafka-docs from GitHub Packages - derived here from `gh auth
# token` if not already set.
#
# Usage: ./scripts/verify-schema-registry-check.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GLUON_DIR="$(cd "$ROOT_DIR/../.." && pwd)"
cd "$ROOT_DIR"

export GITHUB_ACTOR="${GITHUB_ACTOR:-beckfordp}"
export GITHUB_TOKEN="${GITHUB_TOKEN:-$(gh auth token)}"

SERVICE="order-service"
API="http://apicurio-registry.gluon-local.svc.cluster.local:8080/apis/registry/v3/groups/gluon/artifacts"
PRODUCES=(order-events-OrderConfirmed order-events-OrderPaymentFailed order-events-OrderCancelled)
CONSUMES=(payment-events-PaymentCaptured payment-events-PaymentAuthorizationExpired payment-events-PaymentFailed fulfilment-events-DispatchConfirmed fulfilment-events-DispatchFailed)
FAILED=0

fail() { echo "FAIL: $1" >&2; FAILED=1; }
versions() {
  curl -s "$API/$1/versions?limit=1000" |
    python3 -c "import json,sys; print(json.load(sys.stdin).get('count', 0))" 2>/dev/null || echo 0
}
producer_label() {
  curl -s "$API/$1" |
    python3 -c "import json,sys; print((json.load(sys.stdin).get('labels') or {}).get('producer', ''))" 2>/dev/null || true
}
snapshot() {
  for a in ${PRODUCES[@]+"${PRODUCES[@]}"} ${CONSUMES[@]+"${CONSUMES[@]}"}; do
    [ -n "$a" ] && echo "$a=$(versions "$a")"
  done
}

echo "1. Restoring gluon's registry/ export (the baseline CI also restores)..."
"$GLUON_DIR/bin/registry-restore"
BEFORE="$(snapshot)"

echo
echo "2. schemaRegistryCheck against the baseline..."
sbt --error schemaRegistryCheck && echo "   OK: check passed" || fail "schemaRegistryCheck failed"

for a in ${PRODUCES[@]+"${PRODUCES[@]}"}; do
  [ -n "$a" ] || continue
  [ "$(versions "$a")" -ge 1 ] || fail "$a not registered"
  label="$(producer_label "$a")"
  [ "$label" = "$SERVICE" ] && echo "   OK: $a registered, producer=$SERVICE" || fail "$a producer label is '$label'"
done
for a in ${CONSUMES[@]+"${CONSUMES[@]}"}; do
  [ -n "$a" ] || continue
  before="$(echo "$BEFORE" | grep "^$a=" | cut -d= -f2)"
  after="$(versions "$a")"
  [ "$before" = "$after" ] && echo "   OK: consumed $a not written ($after version(s))" || fail "consumed $a went $before -> $after versions"
  label="$(producer_label "$a")"
  [ "$label" != "$SERVICE" ] || fail "consumed $a is labelled producer=$SERVICE"
done
AFTER_FIRST="$(snapshot)"

echo
echo "3. Second, identical run (no new versions)..."
sbt --error schemaRegistryCheck && echo "   OK: second run passed" || fail "second run failed"
[ "$(snapshot)" = "$AFTER_FIRST" ] && echo "   OK: no new versions" || fail "second run changed version counts"

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks failed - see FAIL lines above." >&2
  exit 1
fi
