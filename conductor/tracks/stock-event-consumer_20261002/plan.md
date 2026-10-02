# Implementation Plan: US-5.2 consume stock-reservation events, update order status

## Phase 1: Kafka config + atomic per-item status update (self-contained, no existing signatures change) [checkpoint: a4aa4d6]

- [x] Task: Add `fs2-kafka` + `testcontainers-scala-kafka` dependencies; add `kafka` block to `application.conf` + matching `KafkaConfig`/`OrderServiceConfig` field, mirroring inventory-service's shape `1d331f3`
- [x] Task: Add `OrderStore.updateStatusByItemId(orderItemId: String, newStatus: OrderStatus): F[Boolean]` (atomic conditional UPDATE joining through `order_items.id`, both in-memory and Postgres impls); unit tests covering: matching `Pending` order flips, unknown item id no-ops, non-`Pending` order no-ops `b096f40`
- [x] Task: Conductor - User Manual Verification 'Kafka config + atomic per-item status update' (Protocol in workflow.md) `a4aa4d6`

## Phase 2: Wire orderItemId into the synchronous reserve call [checkpoint: 3c5a741]

- [x] Task: Add `orderItemId: String` param to `InventoryClient.reserve`; reorder `OrderRoutes`' checkout to call `store.create` before `reserveAll`, passing each persisted `OrderItem.id` as the correlation id; update `Main.scala`'s wiring `e29e93f`
- [x] Task: Update `InventoryClientSuite`/`OrderRoutesSuite`/`OrderDocsSuite` call sites forced by the signature changes `7ba7e90`
- [x] Task: Conductor - User Manual Verification 'Wire orderItemId into the synchronous reserve call' (Protocol in workflow.md) `3c5a741`

## Phase 3: Kafka consumer (the actual US-5.2 behavior)

- [x] Task: Add local `StockReservedEvent`/`StockReservationFailedEvent` case classes (mirroring inventory-service's payload, no shared library) and a consumer that subscribes to both topics, calling `updateStatusByItemId` with `Reserved`/`ReservationFailed`; wire it as a background stream in `Main.scala` `e442ed2`
- [ ] Task: Testcontainers-Kafka tests: publish synthetic events for a pre-created `Pending` order's item, assert status flips to `Reserved`/`ReservationFailed`; an event for an unknown/already-resolved item is a no-op
- [ ] Task: Conductor - User Manual Verification 'Kafka consumer' (final, Protocol in workflow.md)

Three phases, each independently testable before the next touches it -
Phase 1 is pure addition, Phase 2 touches existing checkout signatures
(forces call-site updates, likely one combined commit per the usual Scala
whole-module-compile constraint), Phase 3 is the new consumer built on top
of both.
