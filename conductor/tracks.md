# Project Tracks

This file tracks all major tracks for the project.

- [ ] **Track: Add an order_items table by hand**
  *Link: [./tracks/order-items-table_20261001/](./tracks/order-items-table_20261001/)*

---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- Add the FK constraint on `order_items.order_id` → `"order"(id)`, `ON DELETE CASCADE` — same database as `order_items`, so this is a real, enforced Postgres constraint, not just an application-level reference (infra)
- US-3.1: checkout creates an order
- US-4.2: wire resilience middleware for the reserve call to inventory-service
- US-5.2: consume inventory.stock-reserved / inventory.stock-reservation-failed, update order status
- US-8.1: order history read endpoint + Redis cache

---
