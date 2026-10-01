# Project Tracks

This file tracks all major tracks for the project.

- [x] **Track: Harden `status` from raw String to a real enum {pending, reserved, reservation_failed}**
  *Link: [./tracks/status-enum_20261001/](./tracks/status-enum_20261001/)*

---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- Add an `order_items` table by hand (Flyway migration `V2__create_order_items.sql`, following the V1 naming pattern) — nested/collection fields aren't codegen-able, so line items sit outside the codegen'd `order` entity entirely. Columns: `id UUID PRIMARY KEY` (app-generated, `UUID.randomUUID()`, matching the base entity's own convention — no DB-side default), `order_id UUID NOT NULL`, `sku`/`product_name`/`unit_price_cents` snapshotted at order time (no real FK to catalog-service — different service, different database, one-db-per-service — so this is a logical reference only; snapshotting means order history shows the price/name as purchased, not today's catalog values), `quantity INT NOT NULL` (infra)
- Add the FK constraint on `order_items.order_id` → `"order"(id)`, `ON DELETE CASCADE` — same database as `order_items`, so this is a real, enforced Postgres constraint, not just an application-level reference (infra)
- US-3.1: checkout creates an order
- US-4.2: wire resilience middleware for the reserve call to inventory-service
- US-5.2: consume inventory.stock-reserved / inventory.stock-reservation-failed, update order status
- US-8.1: order history read endpoint + Redis cache

---
