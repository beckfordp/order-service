#!/usr/bin/env bash
set -euo pipefail

# Verifies order-service's schemaRegistryCheck sbt task end-to-end (Rework 5
# step 3, ADR 0003) against the real local Apicurio Registry instance
# (gluon's infra/k8s/local-infra). Unlike purerest's own equivalent script
# (which drives a throwaway fixture in a disposable sbt project), this one
# exercises the real production AsyncApiChannels registrations in
# AsyncApiChannels.scala, registering all 6 of this service's actual event
# schemas under the real "order-service" group.
#
# Proves: a run registers all 6 channels and sets a BACKWARD compatibility
# rule on each (first time only); a second identical run is a no-op (no
# error).
#
# Requires GITHUB_ACTOR/GITHUB_TOKEN (read:packages) to resolve
# sbt-purekafka-docs from GitHub Packages - derived here from `gh auth
# token` if not already set.
#
# Usage: ./scripts/verify-schema-registry-check.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export GITHUB_ACTOR="${GITHUB_ACTOR:-beckfordp}"
export GITHUB_TOKEN="${GITHUB_TOKEN:-$(gh auth token)}"

FAILED=0

echo "1. First run (registers all 6 channels + sets the BACKWARD rule)..."
if sbt --error schemaRegistryCheck; then
  echo "   OK: first run succeeded"
else
  echo "FAIL: first run should have succeeded" >&2
  FAILED=1
fi

echo
echo "2. Checking the registry directly for all 6 registered artifacts..."
ARTIFACTS_URL="http://apicurio-registry.gluon-local.svc.cluster.local:8080/apis/registry/v3/groups/order-service/artifacts"
RESPONSE="$(curl -s "$ARTIFACTS_URL")"
for needle in "inventory-events-StockReserved" "inventory-events-StockReservationFailed" "payment-events-PaymentSettled" "payment-events-PaymentFailed" "order-events-OrderReserved" "order-events-OrderStatusChanged"; do
  if ! echo "$RESPONSE" | grep -q "$needle"; then
    echo "FAIL: registry missing expected artifact: ${needle}" >&2
    FAILED=1
  else
    echo "   OK: registry contains artifact \"${needle}\""
  fi
done

echo
echo "3. Second, identical run (should be a no-op, not an error)..."
if sbt --error schemaRegistryCheck; then
  echo "   OK: second run succeeded"
else
  echo "FAIL: second identical run should have succeeded" >&2
  FAILED=1
fi

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
  exit 0
else
  echo "One or more checks failed - see FAIL lines above." >&2
  exit 1
fi
