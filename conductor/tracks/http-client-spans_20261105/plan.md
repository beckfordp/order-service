# Plan: PERF-5.10 — HTTP client spans (ClientTracing) (gluon PERF-5.10)

## Phase 1: Tracing
- [ ] Task: purerestlib/purekafka → 0.14.0; `ClientTracing.middleware` on the Main http client
- [ ] Task: Full `sbt scalafmtCheck test`
- [ ] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md):
      redeploy, acceptance, cross-service trace in Tempo
