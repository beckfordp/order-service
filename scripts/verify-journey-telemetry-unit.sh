#!/usr/bin/env bash
set -euo pipefail

# Phases 1-2 verification for journey-telemetry_20261103 (gluon PERF-4):
# scalafmt + the full test suite, then coverage on the classes this track adds
# or changes (> 80%, workflow target).
#
# Usage: ./scripts/verify-journey-telemetry-unit.sh
# Needs GITHUB_ACTOR/GITHUB_TOKEN (read:packages) for purerestlib, or a
# logged-in `gh`.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
export GITHUB_ACTOR="${GITHUB_ACTOR:-beckfordp}"
export GITHUB_TOKEN="${GITHUB_TOKEN:-$(gh auth token)}"
LOG="target/verify-journey-telemetry-unit.log"
mkdir -p target

echo "== sbt scalafmtCheck test + coverage (log: $LOG) =="
sbt -batch scalafmtCheck Test/scalafmtCheck coverage test coverageReport \
  > "$LOG" 2>&1 < /dev/null || {
  echo "FAIL: build/test failed — see $LOG" >&2
  exit 1
}
grep -E "Passed: Total" "$LOG" | sed 's/^/  /'

python3 - target/scala-3.9.0/scoverage-report/scoverage.xml "$@" <<'PY' || exit 1
import sys, xml.etree.ElementTree as ET
new = set(sys.argv[2:]) or {"orderservice.JourneyMetrics", "orderservice.CheckoutOutcomes"}
failed, seen = False, set()
for c in ET.parse(sys.argv[1]).getroot().iter("class"):
    name, rate = c.get("name"), float(c.get("statement-rate"))
    if name in new:
        seen.add(name)
        ok = rate > 80
        failed |= not ok
        print(f"  {'PASS' if ok else 'FAIL'} {name} {rate:.2f}%")
missing = new - seen
if missing:
    print(f"  FAIL no coverage data for {sorted(missing)}")
sys.exit(1 if failed or missing else 0)
PY

echo "PASS: journey telemetry unit gates"
