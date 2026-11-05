# Plan: traceparent into the outbox (gluon PERF-5.4)

## Phase 1: Write trace_parent
- [ ] Task: purerestlib/purekafka → 0.11.0 in `build.sbt`
- [ ] Task: Test: outbox row inside a span carries its traceparent; outside, NULL
- [ ] Task: `insertOutboxEvent` + `trace_parent`; `insertOutbox` helper; store takes the tracer; `Main` passes it
- [ ] Task: Full `sbt scalafmtCheck test`
- [ ] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md):
      redeploy, row and Kafka header carry the traceparent
