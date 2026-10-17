#!/usr/bin/env bash
set -euo pipefail

# Generates static AsyncAPI HTML docs for order-service's registered Kafka
# event contracts (ADR 0010's AsyncAPI follow-up, migrated from purerest's
# kafka-harness reference implementation per order-service's own
# asyncapi-docs_20261017 track).
#
# Fetches the running spec from order-service's own /docs/asyncapi.json
# route, saves it to disk, then runs the official @asyncapi/generator +
# @asyncapi/html-template (Node.js/npm, via npx) to render static HTML.
# order-service must already be running (e.g. `sbt run`, with its
# docker-compose dependencies up).
#
# Usage: ./scripts/generate-kafka-asyncapi-docs.sh
# Env:   ORDER_SERVICE_URL (default http://localhost:8080)
#        OUTPUT_DIR (default target/asyncapi-docs)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ORDER_SERVICE_URL="${ORDER_SERVICE_URL:-http://localhost:8080}"
OUTPUT_DIR="${OUTPUT_DIR:-target/asyncapi-docs}"
SPEC_FILE="target/asyncapi-docs-spec/asyncapi.json"

mkdir -p "$(dirname "$SPEC_FILE")"

echo "1. Fetching the running spec from ${ORDER_SERVICE_URL}/docs/asyncapi.json..."
http_status="$(curl -s -o "$SPEC_FILE" -w '%{http_code}' "${ORDER_SERVICE_URL}/docs/asyncapi.json")"
if [ "$http_status" != "200" ]; then
  echo "FAIL: expected HTTP 200 fetching the spec, got ${http_status}. Is order-service running?" >&2
  exit 1
fi
echo "   Saved to ${SPEC_FILE}"

echo "2. Generating static HTML via @asyncapi/cli (generate fromTemplate) + @asyncapi/html-template..."
# Same version pins and patched-template workaround as purerest's
# scripts/generate-kafka-asyncapi-docs.sh (that script's own comments have
# the full why: generator dropped its CLI bin, cli@latest needs Node >=24,
# npx wouldn't symlink the 4.1.1 binary on this machine, html-template
# 2.x+ needs generator >=2.0.0, and 1.0.0's declared apiVersion mismatches
# the parser actually installed unless patched to "v3"). This repo keeps
# its own cached copy under target/ rather than sharing purerest's, since
# the two are separate repos/sandboxes.
ASYNCAPI_CLI_DIR="target/asyncapi-cli-tool"
ASYNCAPI_BIN="${ASYNCAPI_CLI_DIR}/node_modules/.bin/asyncapi"
if [ ! -x "$ASYNCAPI_BIN" ]; then
  echo "   (first run — installing @asyncapi/cli@4.1.1 into ${ASYNCAPI_CLI_DIR}, cached for next time)"
  mkdir -p "$ASYNCAPI_CLI_DIR"
  npm install --no-save --prefix "$ASYNCAPI_CLI_DIR" @asyncapi/cli@4.1.1 >/dev/null
fi

mkdir -p "$OUTPUT_DIR"
find "$OUTPUT_DIR" -mindepth 1 -delete

GENERATOR_NODE_MODULES="${ASYNCAPI_CLI_DIR}/node_modules/@asyncapi/generator/node_modules"
HTML_TEMPLATE_ORIGINAL="${GENERATOR_NODE_MODULES}/@asyncapi/html-template"
HTML_TEMPLATE_PATCHED="${GENERATOR_NODE_MODULES}/@asyncapi/html-template-patched"
if [ ! -d "$HTML_TEMPLATE_PATCHED" ]; then
  if [ ! -d "$HTML_TEMPLATE_ORIGINAL" ]; then
    echo "   (installing @asyncapi/html-template@1.0.0 once, to patch it)"
    "$ASYNCAPI_BIN" generate fromTemplate "$SPEC_FILE" @asyncapi/html-template@1.0.0 --output "$(mktemp -d)" --force-write >/dev/null 2>&1 || true
  fi
  cp -R "$HTML_TEMPLATE_ORIGINAL" "$HTML_TEMPLATE_PATCHED"
  python3 -c "
import json
path = '${HTML_TEMPLATE_PATCHED}/package.json'
with open(path) as f:
    data = json.load(f)
data['generator']['apiVersion'] = 'v3'
with open(path, 'w') as f:
    json.dump(data, f, indent=2)
"
fi

"$ASYNCAPI_BIN" generate fromTemplate "$SPEC_FILE" "$HTML_TEMPLATE_PATCHED" --output "$OUTPUT_DIR" --force-write

echo "3. Done — static HTML at ${OUTPUT_DIR}/index.html"
