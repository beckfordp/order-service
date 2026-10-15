# Plan — TD-3.2: Transactional outbox for order-service (ADR 0008)

## Phase 1 — Outbox table + atomic store writes [checkpoint: 494dcf5]
- [x] Task: Write failing `MigrationsSuite` test asserting `outbox_events` exists with columns `id`/`aggregate_id`/`payload`/`trace_parent`/`correlation_id`/`created_at` (Red) `dfc2e13`
- [x] Task: Add `V7__create_outbox_events.sql` migration to make it pass (Green) `dfc2e13`
- [x] Task: Write failing `OrderStoreSuite` tests: `update` (OrderRoutes' checkout-failure path) writes a matching outbox row in the same transaction as its state-change UPDATE (Red) `ed79de1`
- [x] Task: Extend `OrderStore.update` to accept the envelope-building inputs and write the outbox row inside its existing `session.transaction` (Green) `ed79de1`
- [x] Task: Write failing tests: `updateStatusByItemId` writes a matching outbox row atomically (Red); extend it to do so (Green) `ed79de1`
- [x] Task: Write failing tests: `updateStatusIfCurrent` writes a matching outbox row atomically (Red); extend it to do so (Green) `ed79de1`
- [x] Task: Write failing atomicity test: a state-change write that doesn't match (e.g. `updateStatusIfCurrent` with a stale `expected` status, returning `None`) writes no outbox row either (Red); confirm the existing `None`-short-circuit path already satisfies this (Green) `ed79de1`
- [x] Task: Conductor - User Manual Verification 'Phase 1: Outbox table + atomic store writes' (Protocol in workflow.md) `494dcf5` — reduced-prompting mode: automated-test verification only (146 passed), no interactive walkthrough; see git note on `494dcf5`

## Phase 2 — Wire call sites to the outbox, not the publisher [checkpoint: 5922112]
- [x] Task: `OrderRoutes`' checkout-failure path: replace the `publisher.publishStatusChanged(...)` call with passing the envelope inputs into `store.update` (Red: existing route test asserts outbox row rather than publisher invocation; Green: wire it) `c4cbac6`
- [x] Task: `StockEventConsumer`'s `handleReserved`/`handleReservationFailed`: same swap for `updateStatusByItemId` (Red/Green) `c4cbac6`
- [x] Task: `PaymentEventConsumer`'s `handleSettled`/`handleFailed`: same swap for `updateStatusIfCurrent` (Red/Green) `c4cbac6`
- [x] Task: Conductor - User Manual Verification 'Phase 2: Wire call sites to the outbox' (Protocol in workflow.md) `5922112` — reduced-prompting mode: automated-test verification only (146 passed), no interactive walkthrough; see git note on `5922112`

## Phase 3 — Delete OrderEventPublisher
- [x] Task: Delete `OrderEventPublisher.scala` and `OrderEventPublisherSuite.scala` `4f65161`
- [x] Task: Remove `OrderEventPublisher.resource(...).use { ... }` wiring and publisher parameter threading from `Main.scala`, `OrderRoutes`, `StockEventConsumer`, `PaymentEventConsumer` `4f65161`
- [x] Task: Run full test suite, confirm no regressions, confirm nothing still references `OrderEventPublisher` `4f65161`
- [ ] Task: Conductor - User Manual Verification 'Phase 3: Delete OrderEventPublisher' (Protocol in workflow.md)
