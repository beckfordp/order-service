# Plan: PERF-5.10 — HTTP client spans (ClientTracing) (gluon PERF-5.10)

## Phase 1: Tracing
- [x] Task: purerestlib/purekafka → 0.14.0; `ClientTracing.middleware` on the Main http client 4642ace
- [x] Task: Full `sbt scalafmtCheck test` (262 green); fixed flaky PERF-5.5 consumer test (shared topic → select span by trace id) 4642ace
- [x] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md):
      Redeployed in pairs (TD-31). Acceptance: run 1 seeding hit TD-26, run 2 51/53 during
      a host overload (load 48, k8s API down), run 3 53/53, ledger OK, 0 restarts. Tempo:
      checkout trace 88af97ce… order CLIENT → inventory/payment SERVER children, fulfilment
      CLIENT → order SERVER; dispatch trace a155c23f… notification CLIENT → order + user SERVER.

## Review
- No findings: one-line wiring in Main; behaviour covered by purerest's ClientTracingSuite
  and the live traces above.
