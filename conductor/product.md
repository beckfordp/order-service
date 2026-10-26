# Product Guide — order-service

## Context
Part of the **Gluon** platform (v1) — a production-grade microservices
e-commerce platform built pure-FP-first in Scala 3 (Cats Effect / http4s).
See the cross-repo [Gluon Product Vision](../../../docs/product.md) (in the
`gluon/` monorepo root) for the full platform vision, naming scheme, and
goals. This document scopes that vision down to order-service's own slice.

## What this service does
order-service owns the **Order** domain entity — what a customer is buying,
its price snapshot, and its fulfillment status — for the walking-skeleton
checkout flow in Gluon's user stories. It is the only service permitted to
read or write the `order` Postgres database (one-db-per-service).

Generated via `pure-service-generator` (giter8 template over `purerest`),
field-spec applied from `gluon/specs/order.yaml`, then hand-extended per
`gluon/backlogs/order-service.md`.

## Domain model
- **Order** — `customerId` (create-only), `totalCents` (computed server-side
  from its items' `quantity × unitPriceCents`, summed — not client-supplied),
  `status` (server-defaulted `"pending"`; hardened to a closed `OrderStatus`
  ADT — `Pending`/`Reserved`/`ReservationFailed`/`Confirmed`/`PaymentFailed`/
  `Cancelled`
  — at both the Scala and DB level: a `text.eimap`-based Skunk codec plus a
  Postgres `CHECK` constraint. PATCH/PUT reject any other value with `400`)
- **OrderItem** (hand-added — not codegen'd) — `id`, `orderId`, `sku`,
  `productName`, `unitPriceCents`, `quantity`, all snapshotted at
  order-create time; `order_items.order_id` → `"order"(id)`
  `ON DELETE CASCADE` enforced as a real Postgres FK (same database as
  `order`). Checkout (`POST /orders`) requires at least one item and rejects
  a non-positive `quantity`/negative `unitPriceCents` with `400`; an order
  and all its items are persisted atomically in one DB transaction. No FK to
  catalog-service — different service, different database, logical
  reference + snapshot only
- **Stock reservation** — checkout synchronously calls inventory-service's
  reserve endpoint per item via `InventoryClient`, wrapped in purerest's
  `Resilience.middleware` (retry + circuit breaker). All-or-nothing: the
  order and its items are always persisted, but `status` becomes
  `reservation_failed` if any item can't be reserved or the call fails;
  otherwise it stays `pending`. No compensating release of already-reserved
  items on partial failure (inventory-service has no such endpoint); partial
  order fulfillment is a flagged future improvement, not yet designed. The
  synchronous checkout response (`POST /orders` only) additionally carries
  an ephemeral `reservationFailure: {sku, reason}` field when `status`
  becomes `reservation_failed` on this call — naming the specific item and
  the already-computed reason (insufficient stock / unknown sku / the raw
  reservation-call error). Not persisted: a later `GET` of the same order
  omits it. Not populated for orders flipped to `reservation_failed` by the
  async `StockEventConsumer` path below.
  Checkout persists the order (minting real `order_items.id`s) before
  calling reserve, so each reserve call carries its item's real id as
  `orderItemId` — sent to inventory-service and echoed back on its async
  `StockReserved`/`StockReservationFailed` events (both on the
  `inventory-events` domain topic, wrapped in the shared envelope — one topic
  per domain, ADR 0009, migrated 2026-10-11, kafka-envelope_20261011;
  previously two per-event-type topics). A background Kafka consumer
  (`StockEventConsumer`) subscribes to `inventory-events` and flips a
  still-`pending` order to `reserved`/`reservation_failed` by exact
  `orderItemId` match — a single correctly-attributed event is sufficient,
  since the synchronous call already verified every item's outcome before
  leaving the order `pending`. Event types this consumer doesn't recognize
  are silently skipped, not logged as malformed
- **Payment outcomes** — a background Kafka consumer
  (`PaymentEventConsumer`) subscribes to the `payment-events` domain topic
  and listens for payment-service's `PaymentCaptured`,
  `PaymentAuthorizationExpired`, and `PaymentFailed` envelopes: capture flips
  a `Reserved` order to `confirmed`, an expired authorization flips it to the
  new `cancelled` status, and a capture failure flips it to `payment_failed`,
  all by exact `orderId` match (unlike the stock-reservation consumer, which
  matches by `orderItemId`). Publishes `OrderStatusChanged` on a successful
  transition. A redelivered/out-of-order event for an order that's no longer
  `Reserved` (or an unknown `orderId`) is an idempotent no-op.
  `PaymentAuthorized` is deliberately not consumed — authorization is the
  synchronous REST call now (Rework 7).
- **Order history** — `GET /orders?customerId=<id>` (required query param,
  no auth) returns that customer's orders newest-first as
  `List[OrderResponse]` (same shape as `GET /orders/{id}`). Cache-aside via
  Redis (`OrderHistoryCache`, `redis4cats`): a hit returns the cached list; a
  miss queries `OrderStore.listByCustomer`, caches the result with a
  configurable TTL, and returns it. TTL-only freshness — no explicit
  invalidation when an order is created or its status changes, so a write
  can take up to the TTL to show up in a cached list. First Redis
  integration anywhere in Gluon

## User stories in scope (gluon/docs/user-stories.md)
- US-3.1 — checkout creates an order
- US-4.2 — wire resilience middleware for the reserve call to inventory-service
- US-5.2 — consume `inventory.stock-reserved` / `inventory.stock-reservation-failed`, update order status
- US-5.3 — publish `order.reserved` once an order's stock is fully reserved
- US-5.4 — publish `order.status-changed` (`reservation_failed`) when a
  reservation fails, from either the synchronous checkout-failure path or
  the async consumer path
- US-6.3 — consume `payment.settled` / `payment.failed`, update order
  status to `confirmed` / `payment_failed`, publish `order.status-changed`
- US-8.1 — order history read endpoint + Redis cache

## Sequencing (gluon/PLAN.md)
- **Phase 1** (parallel with inventory-service) — US-3.1
- **Phase 2** (depends on Phase 1) — US-4.2, resilience-wrapped sync reserve call
- **Phase 3** (parallel with inventory-service) — US-5.2, consume Kafka events;
  US-5.3, publish `order.reserved` (order-service's first Kafka producer);
  US-5.4, publish `order.status-changed` on reservation failure (reuses the
  same producer)
- **Phase 4** (depends on payment-service's US-6.1 actually publishing) —
  US-6.3, consume `payment.settled`/`payment.failed`, update order status to
  `confirmed`/`payment_failed`, publish `order.status-changed` (reuses
  US-5.3/US-5.4's `OrderEventPublisher`)
- **Phase 8** (independent — can run anytime) — US-8.1, order history + Redis cache

## Events
- Publishes: `order-events` (eventType `OrderReserved` / `OrderStatusChanged`,
  wrapped in the shared envelope — one topic per domain, ADR 0009, migrated
  2026-10-11, kafka-envelope_20261011; previously two per-event-type topics)
- Consumes: `inventory-events` (eventType `StockReserved` /
  `StockReservationFailed`), `payment-events` (eventType `PaymentCaptured` /
  `PaymentAuthorizationExpired` / `PaymentFailed`; `PaymentAuthorized` is
  learned synchronously) — same migration

## Out of scope for this service
- Auth/identity (bare `customerId` for now — no user-service yet)
- Payment processing (payment-service's job)
- Catalog data (catalog-service's job; order_items snapshots catalog values
  at order-create time only, never looks them up live)
