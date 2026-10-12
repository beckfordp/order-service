# Plan — TD-3.2: Transactional outbox for order-service (ADR 0008)

## Phase 1 — Outbox table + atomic store writes
- [ ] Task: Write failing `MigrationsSuite` test asserting `outbox_events` exists with columns `id`/`aggregate_id`/`payload`/`trace_parent`/`correlation_id`/`created_at` (Red)
- [ ] Task: Add `V7__create_outbox_events.sql` migration to make it pass (Green)
- [ ] Task: Write failing `OrderStoreSuite` tests: `update` (OrderRoutes' checkout-failure path) writes a matching outbox row in the same transaction as its state-change UPDATE (Red)
- [ ] Task: Extend `OrderStore.update` to accept the envelope-building inputs and write the outbox row inside its existing `session.transaction` (Green)
- [ ] Task: Write failing tests: `updateStatusByItemId` writes a matching outbox row atomically (Red); extend it to do so (Green)
- [ ] Task: Write failing tests: `updateStatusIfCurrent` writes a matching outbox row atomically (Red); extend it to do so (Green)
- [ ] Task: Write failing atomicity test: a state-change write that doesn't match (e.g. `updateStatusIfCurrent` with a stale `expected` status, returning `None`) writes no outbox row either (Red); confirm the existing `None`-short-circuit path already satisfies this (Green)
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Outbox table + atomic store writes' (Protocol in workflow.md)

## Phase 2 — Wire call sites to the outbox, not the publisher
- [ ] Task: `OrderRoutes`' checkout-failure path: replace the `publisher.publishStatusChanged(...)` call with passing the envelope inputs into `store.update` (Red: existing route test asserts outbox row rather than publisher invocation; Green: wire it)
- [ ] Task: `StockEventConsumer`'s `handleReserved`/`handleReservationFailed`: same swap for `updateStatusByItemId` (Red/Green)
- [ ] Task: `PaymentEventConsumer`'s `handleSettled`/`handleFailed`: same swap for `updateStatusIfCurrent` (Red/Green)
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Wire call sites to the outbox' (Protocol in workflow.md)

## Phase 3 — Delete OrderEventPublisher
- [ ] Task: Delete `OrderEventPublisher.scala` and `OrderEventPublisherSuite.scala`
- [ ] Task: Remove `OrderEventPublisher.resource(...).use { ... }` wiring and publisher parameter threading from `Main.scala`, `OrderRoutes`, `StockEventConsumer`, `PaymentEventConsumer`
- [ ] Task: Run full test suite, confirm no regressions, confirm nothing still references `OrderEventPublisher`
- [ ] Task: Conductor - User Manual Verification 'Phase 3: Delete OrderEventPublisher' (Protocol in workflow.md)
