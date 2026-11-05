# Plan: consumers continue the trace (gluon PERF-5.5)

## Phase 1: Consumer spans
- [x] Task: purerestlib/purekafka → 0.12.0 in `build.sbt` 1c84cfd
- [x] Task: Test: a record's `traceparent` becomes the parent of its `<topic> process` span 1c84cfd
- [x] Task: Wrap each consumer's handling in `KafkaTracing.traced`; thread the tracer from `Main` 1c84cfd
- [x] Task: Full `sbt scalafmtCheck test` (260 tests, green)
- [ ] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md):
      redeploy, consumer spans join producer traces in Tempo
