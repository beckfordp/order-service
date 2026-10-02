# Overview
Implements US-5.2: consume `inventory.stock-reserved`/`inventory.stock-reservation-failed`
and update the owning order's status, using the `orderItemId` correlation id
landed in inventory-service's companion track. Also finishes wiring that id
into order-service's own synchronous call (`InventoryClient`), which requires
reordering checkout to persist the order (minting real `order_items.id`s)
before calling reserve, since the id must exist to send.

# Functional Requirements
- `InventoryClient.reserve` gains an `orderItemId: String` parameter, sent in
  the request body alongside `sku`/`quantity`
- `OrderRoutes`' checkout persists the order via `OrderStore.create` first,
  then calls `reserveAll` using the real, persisted `OrderItem.id`s
  (previously reversed — reserved before persisting). Reservation-failure
  handling (set `ReservationFailed`) otherwise unchanged
- New `OrderStore.updateStatusByItemId(orderItemId: String, newStatus: OrderStatus): F[Boolean]`
  — a single atomic conditional UPDATE transitioning the order owning that
  item from `Pending` to `newStatus`, returning whether it matched. No-op
  (`false`) if the item doesn't exist, its order isn't `Pending`, or is
  already resolved
- New Kafka consumer (mirrors `StockReservedEvent`/`StockReservationFailedEvent`
  locally — same shape as inventory-service's payload, no shared library)
  subscribing to both `inventory.stock-reserved` and
  `inventory.stock-reservation-failed`: success -> `updateStatusByItemId(orderItemId, Reserved)`,
  failure -> `updateStatusByItemId(orderItemId, ReservationFailed)`. A
  `false` result is an idempotent no-op, logged at info, never an error
- Consumer runs as a background stream in `Main.scala` alongside the HTTP
  server, fs2-kafka, at-least-once offset commits (commit after successful
  processing)

# Non-Functional Requirements
- New `fs2-kafka` + `testcontainers-scala-kafka` dependencies, mirroring
  inventory-service's own versions/config shape (`KafkaConfig(bootstrapServers: String)`)
- Consumer tests publish synthetic events directly to a test Kafka
  (Testcontainers), no live inventory-service involved
- `OrderStatus` unchanged (`Pending`/`Reserved`/`ReservationFailed` already
  exist) — no migration needed

# Acceptance Criteria
- [ ] `InventoryClient.reserve` sends `orderItemId`; `InventoryClientSuite`
  updated accordingly
- [ ] Checkout persists the order before calling reserve; existing
  `OrderRoutesSuite` reservation-outcome tests still pass with the reordered
  flow
- [ ] `OrderStore.updateStatusByItemId` flips a `Pending` order to
  `Reserved`/`ReservationFailed` for a matching item id; no-ops for an
  unknown item id or a non-`Pending` order — tested against both in-memory
  and Postgres
- [ ] A synthetic `stock-reserved` event flips a `Pending` order's status to
  `Reserved`
- [ ] A synthetic `stock-reservation-failed` event flips it to
  `ReservationFailed`
- [ ] An event for an unknown/already-resolved item is consumed without
  error
- [ ] `sbt scalafmtCheck test` passes

# Out of Scope
- New `OrderStatus.Confirmed` case (separate, not-yet-scheduled
  payment-settlement gap)
- `order.created`/`order.status-changed` publishing (separate, deliberately
  deferred gap)
- Per-item confirmation tracking / waiting for all items before flipping
  status — one correctly-attributed event suffices, since US-4.2's
  synchronous call already verified every item succeeded before leaving the
  order `Pending`
- Dead-lettering/retry-with-backoff for consumer processing failures —
  default fs2-kafka at-least-once redelivery on restart accepted as
  sufficient
