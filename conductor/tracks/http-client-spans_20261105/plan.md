# Plan: PERF-5.10 — HTTP client spans (ClientTracing) (gluon PERF-5.10)

## Phase 1: Tracing
- [x] Task: purerestlib/purekafka → 0.14.0; `ClientTracing.middleware` on the Main http client 4642ace
- [x] Task: Full `sbt scalafmtCheck test` (262 green); fixed flaky PERF-5.5 consumer test (shared topic → select span by trace id) 4642ace
- [ ] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md):
      redeploy, acceptance, cross-service trace in Tempo
