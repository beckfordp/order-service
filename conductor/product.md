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
- **Order** (generated) — `customerId` (create-only), `totalCents`
  (create-only), `status` (server-defaulted `"pending"`; a `String`
  stand-in for now — codegen v1 has no enum type, so hardening this to a
  real enum (`pending`/`reserved`/`reservation_failed`) is a backlog item)
- **OrderItem** (planned, hand-added — not codegen'd) — line items
  snapshotted at order-create time (`sku`/`product_name`/`unit_price_cents`),
  FK `order_items.order_id` → `"order"(id)` `ON DELETE CASCADE` (same
  database, real constraint); no FK to catalog-service — different service,
  different database, logical reference + snapshot only

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
