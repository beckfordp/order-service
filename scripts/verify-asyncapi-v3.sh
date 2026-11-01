#!/usr/bin/env bash
# Verifies asyncapi-v3_20261101 (gluon TD-19.3/19.4): with purekafka 0.6.0
# the generated AsyncAPI document is 3.0.0 and passes `asyncapi validate`,
# each topic's operation has the right send/receive action and shows in the
# rendered HTML, consumed events carry descriptions copied from the
# producer's registry artifact, and schemaRegistryCheck still passes against
# gluon's restored registry (descriptions don't affect compatibility).
# Since asyncapi-ordering_20261101 (gluon TD-19.6, purekafka 0.6.1): no
# receive operation precedes a send.
#   scripts/verify-asyncapi-v3.sh   (needs Node >= 24.11 and the local k8s Apicurio)
set -euo pipefail
cd "$(dirname "$0")/.."

export GITHUB_ACTOR="${GITHUB_ACTOR:-beckfordp}"
export GITHUB_TOKEN="${GITHUB_TOKEN:-$(gh auth token)}"

# operation key -> action
OPERATIONS=(payment-events=receive fulfilment-events=receive order-events=send)
# topic/eventType of each consumed event
CONSUMED=(payment-events/PaymentCaptured payment-events/PaymentAuthorizationExpired payment-events/PaymentFailed
  fulfilment-events/DispatchConfirmed fulfilment-events/DispatchFailed)

SPEC="target/asyncapi-docs-spec/asyncapi.json"
HTML="target/asyncapi-docs/index.html"
CLI="target/asyncapi-cli-tool/cli-6.2.2_html-3.5.6/node_modules/.bin/asyncapi"

node -e 'const [a,b]=process.versions.node.split(".").map(Number); process.exit(a>24||(a===24&&b>=11)?0:1)' \
  || { echo "FAIL: run on Node >= 24.11 (nvm use 26); found $(node -v)"; exit 1; }
mkdir -p target

echo "== tests, formatting, generated docs =="
sbt -batch -Dsbt.supershell=false scalafmtCheck Test/scalafmtCheck test asyncApiGenerate \
  < /dev/null > target/verify-asyncapi-v3-sbt.log 2>&1 \
  || { echo "FAIL: sbt (see target/verify-asyncapi-v3-sbt.log)"; exit 1; }
echo "ok   tests, scalafmt, asyncApiGenerate"

[ "$(jq -r .asyncapi "$SPEC")" = "3.0.0" ] || { echo "FAIL: spec is not 3.0.0"; exit 1; }
SUPPRESS_NO_CONFIG_WARNING=true "$CLI" validate "$SPEC" < /dev/null > target/verify-asyncapi-v3-validate.log 2>&1 \
  || { echo "FAIL: asyncapi validate (see target/verify-asyncapi-v3-validate.log)"; exit 1; }
echo "ok   spec is 3.0.0 and validates"

expected="$(printf '%s\n' "${OPERATIONS[@]}" | sort)"
actual="$(jq -r '.operations | to_entries[] | "\(.key)=\(.value.action)"' "$SPEC" | sort)"
[ "$expected" = "$actual" ] || { echo "FAIL: operations"; echo "expected: $expected"; echo "actual:   $actual"; exit 1; }
for op in "${OPERATIONS[@]}"; do
  anchor="operation-${op#*=}-${op%%=*}"
  grep -q "$anchor" "$HTML" || { echo "FAIL: HTML missing $anchor"; exit 1; }
done
echo "ok   operations: ${OPERATIONS[*]}"
actions="$(jq -r '[.operations[].action] | join(" ")' "$SPEC")"
[[ "$actions" =~ ^(send ?)*(receive ?)*$ ]] || { echo "FAIL: receive before send: $actions"; exit 1; }
echo "ok   sends before receives: ${actions:-none}"

for c in ${CONSUMED[@]+"${CONSUMED[@]}"}; do
  desc="$(jq -r --arg t "${c%%/*}" --arg e "${c#*/}" '.channels[$t].messages[$e].payload.description // empty' "$SPEC")"
  [ -n "$desc" ] || { echo "FAIL: consumed $c has no description"; exit 1; }
done
echo "ok   ${#CONSUMED[@]} consumed events described"

if [ -x scripts/verify-schema-registry-check.sh ]; then
  ./scripts/verify-schema-registry-check.sh < /dev/null > target/verify-asyncapi-v3-registry.log 2>&1 \
    || { echo "FAIL: verify-schema-registry-check.sh (see target/verify-asyncapi-v3-registry.log)"; exit 1; }
  echo "ok   schemaRegistryCheck against gluon's restored registry"
fi

echo "PASS: AsyncAPI 3.0 docs verified"
