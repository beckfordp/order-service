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
  `shippingAddress` (`line1`, optional `line2`, `city`, `postalCode`,
  `country`; a JSONB column, null on pre-Journey-1 rows), `status` (born
  `authorized` at checkout; hardened to a closed `OrderStatus` ADT —
  `Authorized`/`Confirmed`/`PaymentFailed`/`Cancelled`, plus legacy
  `Pending`/`Reserved`/`ReservationFailed` kept readable for old rows — at
  both the Scala and DB level: a `text.eimap`-based Skunk codec plus a
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
- **Checkout** (ADR 0018, Checkout Journey 1) — `POST /orders` takes
  `items`, `shippingAddress` and `paymentMethodId`, validates them (`400`),
  mints the order and item ids, then: (1) reserves the whole order in one
  call to inventory-service's `POST /inventorys/order-reservations`
  (all-or-nothing; `409` with per-sku `reservationFailures` if any line
  can't be reserved); (2) authorizes the total via payment-service's
  `POST /payments/authorize` (`PaymentClient`; `402` declined with
  `paymentFailure.reason`, `400` unknown payment method); (3) only then
  persists the order as `authorized`, items in the same transaction (`201`).
  No order row exists for any failed checkout. Every failure after a
  successful reserve releases it (`POST
  /inventorys/order-reservations/{orderId}/release`, idempotent); a failed
  release is logged, not surfaced. Either downstream unreachable → `503`
  (inventory or payment, named); persist failure → `500`. Both clients are
  wrapped in purerest's `Resilience.middleware` (retry + circuit breaker,
  one breaker each). Checkout publishes no event: payment-service's
  `PaymentAuthorized` announces the authorization
- **Payment outcomes** — a background Kafka consumer
  (`PaymentEventConsumer`) subscribes to the `payment-events` domain topic
  and listens for payment-service's `PaymentCaptured`,
  `PaymentAuthorizationExpired`, and `PaymentFailed` envelopes: capture flips
  an order awaiting capture (`authorized` or `dispatched`) to `confirmed`, an
  expired authorization flips an `authorized` order to `cancelled`, and a
  capture failure flips it to `payment_failed` **and records an
  `order_reconciliation` row** instead of releasing inventory (the goods may
  already be shipped — US-19.3), all by exact `orderId` match. Two accepted
  source states cover the race between this consumer and the independent
  `DispatchConfirmed` consumer. Publishes `OrderStatusChanged` on a successful
  transition. A redelivered/out-of-order event for an order that's no longer
  awaiting capture (or an unknown `orderId`) is an idempotent no-op.
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
- US-5.2/5.3/5.4 — stock-event consumption, `order.reserved`, and
  `reservation_failed` publishing (superseded by ADR 0018: retired in
  Checkout Journey 1)
- US-11.1 — shipping address captured at checkout
- US-14.2 — synchronous payment authorization at checkout (ADR 0011/0018)
- TD-7.3 — whole-order reserve + release compensation; stock-event consumer
  and `OrderReserved` retired (ADR 0018)
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
- **Checkout Journey 1** (after inventory-service's TD-7.1/7.2 and
  payment-service's authorize endpoint) — US-11.1, US-14.2, TD-7.3

## Events
- Publishes: `order-events` (eventType `OrderStatusChanged`, wrapped in the
  shared envelope — one topic per domain, ADR 0009; via the outbox + Debezium
  CDC). `OrderReserved` retired with ADR 0018 (registry artifact deleted);
  `reservation_failed` stays in the status enum only for BACKWARD
  compatibility
- Consumes: `payment-events` (eventType `PaymentCaptured` /
  `PaymentAuthorizationExpired` / `PaymentFailed`; `PaymentAuthorized` is
  learned synchronously). `inventory-events` no longer consumed (ADR 0018)

## Out of scope for this service
- Auth/identity (bare `customerId` for now — no user-service yet)
- Payment processing (payment-service's job)
- Catalog data (catalog-service's job; order_items snapshots catalog values
  at order-create time only, never looks them up live)

- US-17.4 (2026-10-28): `FulfilmentEventConsumer` consumes
  `fulfilment-events: DispatchConfirmed` and moves an `authorized` order to
  `dispatched` via the conditional status update; unknown/non-authorized
  orders are logged and skipped. No `OrderStatusChanged` on dispatch.
  `DispatchFailed` handling is US-18.3 (not yet built).

- US-18.3 (2026-10-28): `FulfilmentEventConsumer` also handles
  `DispatchFailed` — authorized→`dispatch_failed`, then best-effort
  `InventoryClient.release(orderId)` (failures logged for reconciliation).
  No `OrderStatusChanged` on failure.
- US-19.2/19.3 (2026-10-28): `PaymentEventConsumer` transitions
  `authorized`/`dispatched` → `confirmed` on `PaymentCaptured`, and
  `authorized`/`dispatched` → `payment_failed` on `PaymentFailed`, writing an
  `order_reconciliation` row (reason from the event) in the same transaction.
  No inventory release on the capture-failure path — the consumer has no
  `InventoryClient` dependency at all.
