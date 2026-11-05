# Plan: consumers continue the trace (gluon PERF-5.5)

## Phase 1: Consumer spans
- [x] Task: purerestlib/purekafka → 0.12.0 in `build.sbt` 1c84cfd
- [x] Task: Test: a record's `traceparent` becomes the parent of its `<topic> process` span 1c84cfd
- [x] Task: Wrap each consumer's handling in `KafkaTracing.traced`; thread the tracer from `Main` 1c84cfd
- [x] Task: Full `sbt scalafmtCheck test` (260 tests, green)
- [x] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md):
      redeploy, consumer spans join producer traces in Tempo
      Redeployed; acceptance 53/53 + ledger OK, 0 restarts. Every outbox row now
      has `trace_parent` (consumer-written ones were NULL before). One dispatch
      trace (a7197428…): 12 spans, 5 services, 3 Kafka hops, all CONSUMER spans
      parented on the producing span.

## Review
- No High/Medium: commit stays outside the span; tracer threaded explicitly
  (no implicit); the span covers DeadLetter retries (one long span on retry).
- Low (not fixed): span name `<topic> process` is per topic, not per event
  type; filter by `messaging.destination.name` + logs for the event type.
