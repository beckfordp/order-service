# Plan: Checkout Journey 7 — order-service race reconciliation

## Phase 1: Reconcile loser branches

- [ ] Task: Failing consumer tests — `DispatchConfirmed`, `PaymentCaptured`,
      `PaymentFailed` and `AuthorizationExpired` contradictions flag
      reconciliation; duplicates/unknown do not (Red).
- [ ] Task: Flag non-duplicate `IllegalTransition` outcomes in
      `FulfilmentEventConsumer` and `PaymentEventConsumer`, keeping the
      offset commit (Green).
- [ ] Task: Failing store tests for first-wins reconciliation in the
      in-memory interpreter; align `flagReconciliation` and
      `transition(..., reconciliationReason)` with Postgres (Green).
- [ ] Task: Route-level cancel-after-dispatch 409 test.
- [ ] Task: `sbt scalafmtCheck test`.
- [ ] Task: Conductor - User Manual Verification 'Reconcile loser branches'
      (Protocol in workflow.md)

## Review

- Filled after `/conductor:review`.
