# Plan: business spans (gluon PERF-5.9)

## Phase 1: Business spans
- [x] Task: Tests first: span names/attributes/parent/error status 6458ee5
- [x] Task: Implement spans; wire the tracer from `Main` 6458ee5
- [x] Task: Full `sbt scalafmtCheck test` (262 tests, green)
- [x] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md):
      redeploy, business spans visible in Tempo
      Redeployed; acceptance 53/53 + ledger OK (first run hit TD-26, rerun green).
      Dispatch trace f2cd7d22…: 21 spans, 5 services; consumers are children of
      `outbox.write`; `order.status_changed`/`fulfilment.status_changed` events present.

## Review
- No High/Medium: decorator wired outermost (milestone logs + outbox span nest
  inside); attributes low-cardinality (no tracking numbers/reasons); errors set
  span status Error via otel4s default finalization (tested).
- Behaviour change (accepted): `outbox.write` always opens a span, so a row
  written outside a caller span now carries its own root trace, not NULL.
