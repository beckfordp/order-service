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
  ADT — `Pending`/`Reserved`/`ReservationFailed` — at both the Scala and DB
  level: a `text.eimap`-based Skunk codec plus a Postgres `CHECK` constraint.
  PATCH/PUT reject any other value with `400`)
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
  order fulfillment is a flagged future improvement, not yet designed.
  Checkout persists the order (minting real `order_items.id`s) before
  calling reserve, so each reserve call carries its item's real id as
  `orderItemId` — sent to inventory-service and echoed back on its async
  `inventory.stock-reserved`/`inventory.stock-reservation-failed` events. A
  background Kafka consumer (`StockEventConsumer`) listens for those events
  and flips a still-`pending` order to `reserved`/`reservation_failed` by
  exact `orderItemId` match — a single correctly-attributed event is
  sufficient, since the synchronous call already verified every item's
  outcome before leaving the order `pending`
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
- US-8.1 — order history read endpoint + Redis cache

## Sequencing (gluon/PLAN.md)
- **Phase 1** (parallel with inventory-service) — US-3.1
- **Phase 2** (depends on Phase 1) — US-4.2, resilience-wrapped sync reserve call
- **Phase 3** (parallel with inventory-service) — US-5.2, consume Kafka events
- **Phase 8** (independent — can run anytime) — US-8.1, order history + Redis cache

## Events
- Publishes: `order.created`, `order.status-changed`
- Consumes: `inventory.stock-reserved`, `inventory.stock-reservation-failed`

## Out of scope for this service
- Auth/identity (bare `customerId` for now — no user-service yet)
- Payment processing (payment-service's job)
- Catalog data (catalog-service's job; order_items snapshots catalog values
  at order-create time only, never looks them up live)
