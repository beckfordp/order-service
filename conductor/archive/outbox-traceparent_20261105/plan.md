# Plan: traceparent into the outbox (gluon PERF-5.4)

## Phase 1: Write trace_parent
- [x] Task: purerestlib/purekafka → 0.11.0 in `build.sbt` ff3a787
- [x] Task: Test: outbox row inside a span carries its traceparent; outside, NULL ff3a787
- [x] Task: `insertOutboxEvent` + `trace_parent`; `insertOutbox` helper; store takes the tracer; `Main` passes it ff3a787
- [x] Task: Full `sbt scalafmtCheck test` (259 tests, green)
- [x] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md):
      redeploy, row and Kafka header carry the traceparent
      Redeployed; acceptance 53/53 + ledger OK, 0 restarts. HTTP-driven outbox
      rows carry `trace_parent`; consumer-driven rows NULL (expected until
      PERF-5.5). A PaymentAuthorized row's value is on the Kafka message as
      `traceparent` and its trace (`POST /payments/authorize`) is in Tempo.

## Review
- No High/Medium: one `insertOutbox` helper per store, so every insert site
  goes through it (no `prepare(insertOutboxEvent)` left outside it); skunk
  keeps its noop tracer; tests cover in-span and outside-span.
- Low (not fixed): a NULL column becomes a `traceparent` header with a null
  value (Debezium); PERF-5.5's consumer must treat it as absent.
