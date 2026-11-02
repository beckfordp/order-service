# Plan: Checkout Journey 7 — order-service race reconciliation

## Phase 1: Reconcile loser branches

- [x] Task: Failing consumer tests — `DispatchConfirmed`, `PaymentCaptured`,
      `PaymentFailed` and `AuthorizationExpired` contradictions flag
      reconciliation; duplicates/unknown do not (Red). (7fc9a1a)
- [x] Task: Flag non-duplicate `IllegalTransition` outcomes in
      `FulfilmentEventConsumer` and `PaymentEventConsumer`, keeping the
      offset commit (Green). (7fc9a1a)
- [x] Task: Failing store tests for first-wins reconciliation in the
      in-memory interpreter; align `flagReconciliation` and
      `transition(..., reconciliationReason)` with Postgres (Green). (7fc9a1a)
- [x] Task: Route-level cancel-after-dispatch 409 test. (7fc9a1a)
- [x] Task: `sbt scalafmtCheck test` — 203 passed. (7fc9a1a)
- [x] Task: Conductor - User Manual Verification 'Reconcile loser branches'
      (Protocol in workflow.md) — verified by the full suite above.

## Review

- Filled after `/conductor:review`.
