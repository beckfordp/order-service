# Overview
Add the `order_items` table by hand via a new Flyway migration
(`V3__create_order_items.sql`) — nested/collection fields aren't
codegen-able, so line items for an order sit outside the codegen'd `order`
entity entirely. Schema-only: no Scala domain model or store layer yet
(deferred to the checkout track, US-3.1, which will actually write rows).
The FK constraint is a separate backlog item, not included here.

# Functional Requirements
- New Flyway migration `V3__create_order_items.sql`, following V1's
  naming/style conventions
- Columns: `id UUID PRIMARY KEY` (app-generated, no DB default),
  `order_id UUID NOT NULL` (logical reference only, no FK yet),
  `sku TEXT NOT NULL`, `product_name TEXT NOT NULL`,
  `unit_price_cents INT NOT NULL`, `quantity INT NOT NULL` (sku,
  product_name, and unit_price_cents are snapshotted at order-create time)

# Non-Functional Requirements
- No FK constraint yet — `order_id` is a plain `NOT NULL` column for now
- Migration applies cleanly on top of V1 + V2

# Acceptance Criteria
- [ ] New migration creates `order_items` with exactly the six columns above
- [ ] No FK constraint on `order_id`
- [ ] Migration applies cleanly on top of V1 + V2 (MigrationsSuite-style
  test)
- [ ] `sbt scalafmtCheck test` passes

# Out of Scope
- FK constraint on `order_items.order_id` (separate backlog item/track)
- Scala domain model / store layer for order_items (deferred to US-3.1)
- `created_at`/`updated_at` timestamps on `order_items` — not requested in
  the backlog spec
- Any HTTP API changes
