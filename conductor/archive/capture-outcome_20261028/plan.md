# Plan: US-19.2 + US-19.3 — capture outcome

## Phase 1: Migration + store capture-outcome transition

- [x] Task: Failing tests — `recordCaptureOutcome` from `Dispatched` and `Authorized`, `None`/no outbox from other states, failure stores `reconciliationReason`, `reconciliationReason` read, unknown order; Postgres atomic rollback of status + flag when the outbox insert fails (Red) `ddcd157`
- [x] Task: Implement V12, both trait methods and both interpreters (Green) `ddcd157`
- [x] Task: Refactor, `sbt scalafmtCheck test` green `ddcd157`
- [x] Task: Conductor - User Manual Verification 'Migration + store capture-outcome transition' (Protocol in workflow.md) `ddcd157`

## Phase 2: Consumer

- [x] Task: Failing consumer tests — capture/failure re-based to `Dispatched`, `Authorized` race cases, non-transitioning seed outside the two states, reconciliation reason asserted, outbox assertions re-based (Red) `ddcd157`
- [x] Task: Switch `handleCaptured`/`handleFailed` to `recordCaptureOutcome`; refresh doc comments/log messages; comment the structural no-auto-release rule (Green) `ddcd157`
- [x] Task: Refactor, full suite + coverage green `ddcd157`
- [x] Task: Conductor - User Manual Verification 'Consumer' (Protocol in workflow.md) `ddcd157`

## Phase 3: Live proof, docs

- [x] Task: `scripts/verify-capture-outcome.sh` — success -> `confirmed`; duplicate harmless; decline card -> `payment_failed` + reconciliation row + inventory `quantityReserved` unchanged; cleanup `ddcd157`
- [x] Task: Rebuild/redeploy order-service; run the script for real `ddcd157`
- [x] Task: Update `conductor/product.md` (README/tech-stack needed no change); run `schemaRegistryCheck` — blocked by pre-existing registry drift, see Risks `ddcd157`
- [x] Task: Conductor - User Manual Verification 'Live proof, docs' (Protocol in workflow.md) `ddcd157`

## Risks / notes

- Accepting both `authorized` and `dispatched` keeps the state machine
  convergent across cross-topic ordering; later dispatch events for a
  resolved order are logged no-ops.
- Capture failure still writes `OrderStatusChanged{payment_failed}`; US-19.4
  must avoid a duplicate notification email.
- Reconciliation is store-level only (no operator UI/API yet).
- `sbt schemaRegistryCheck` fails for `order-events-OrderStatusChanged`: the
  shared Apicurio artifact currently holds notification-service's 3-value
  mirror (no `cancelled`), while order-service generates 4 values. Registering
  the changed content then hits Apicurio's Everit-based compatibility checker,
  which cannot parse tapir's generated draft-2020-12 `$schema` ("could not
  determine version", 500). Pre-existing cross-repo drift + toolchain
  limitation, not caused by this track; needs its own TD (sync the
  notification-service mirror / reset the artifact, and/or emit draft-07 from
  purekafka).
