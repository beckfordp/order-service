# Plan: US-19.2 + US-19.3 — capture outcome

## Phase 1: Migration + store capture-outcome transition

- [ ] Task: Failing tests — `recordCaptureOutcome` from `Dispatched` and `Authorized`, `None`/no outbox from other states, failure stores `reconciliationReason`, `reconciliationReason` read, unknown order; Postgres atomic rollback of status + flag when the outbox insert fails (Red)
- [ ] Task: Implement V12, both trait methods and both interpreters (Green)
- [ ] Task: Refactor, `sbt scalafmtCheck test` green
- [ ] Task: Conductor - User Manual Verification 'Migration + store capture-outcome transition' (Protocol in workflow.md)

## Phase 2: Consumer

- [ ] Task: Failing consumer tests — capture/failure re-based to `Dispatched`, `Authorized` race cases, non-transitioning seed outside the two states, reconciliation reason asserted, outbox assertions re-based (Red)
- [ ] Task: Switch `handleCaptured`/`handleFailed` to `recordCaptureOutcome`; refresh doc comments/log messages; comment the structural no-auto-release rule (Green)
- [ ] Task: Refactor, full suite + coverage green
- [ ] Task: Conductor - User Manual Verification 'Consumer' (Protocol in workflow.md)

## Phase 3: Live proof, docs

- [ ] Task: `scripts/verify-capture-outcome.sh` — success -> `confirmed`; duplicate harmless; decline card -> `payment_failed` + reconciliation row + inventory `quantityReserved` unchanged; cleanup
- [ ] Task: Rebuild/redeploy order-service; run the script for real
- [ ] Task: Update README / `conductor/product.md` / `tech-stack.md`; run `schemaRegistryCheck`
- [ ] Task: Conductor - User Manual Verification 'Live proof, docs' (Protocol in workflow.md)

## Risks / notes

- Accepting both `authorized` and `dispatched` keeps the state machine
  convergent across cross-topic ordering; later dispatch events for a
  resolved order are logged no-ops.
- Capture failure still writes `OrderStatusChanged{payment_failed}`; US-19.4
  must avoid a duplicate notification email.
- Reconciliation is store-level only (no operator UI/API yet).
