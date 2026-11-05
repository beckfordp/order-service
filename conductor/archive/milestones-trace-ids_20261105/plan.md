# Plan: milestone logs without correlation_id (gluon PERF-5.6)

## Phase 1: Milestones
- [x] Task: purerestlib/purekafka → 0.13.0; drop the `None` argument f205771
- [x] Task: Full `sbt scalafmtCheck test` (Total 262 tests, green)
- [x] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md):
      redeploy, milestone lines carry the business span's ids
      Acceptance 53/53, ledger OK (after TD-31 recovery: Kafka OOM wiped topics).
      100% of app log lines in the run carry trace_id; each milestone line's
      span_id is its business span in Tempo (e.g. order_placed → order.create,
      payment_captured → payment.record_capture).

## Review
- No findings: mechanical; milestone tests assert the log ctx unchanged.
