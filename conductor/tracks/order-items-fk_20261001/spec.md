# Overview
Add the FK constraint on `order_items.order_id` → `"order"(id)`
`ON DELETE CASCADE` via a new Flyway migration (`V4__add_order_items_fk.sql`).
Closes out the FK deferral from the previous track
(`order-items-table_20261001`), which deliberately left `order_id` as a
plain `NOT NULL` column with no referential constraint. Since both tables
live in the same database, this is a real, enforced Postgres constraint —
not just an application-level reference (unlike cross-service references,
e.g. to catalog-service, which stay logical-only per one-db-per-service).

# Functional Requirements
- New Flyway migration `V4__add_order_items_fk.sql`:
  `ALTER TABLE order_items ADD CONSTRAINT order_items_order_id_fkey FOREIGN KEY (order_id) REFERENCES "order"(id) ON DELETE CASCADE;`
- Update the now-obsolete `MigrationsSuite` test from the previous track
  ("order_items.order_id has no FK constraint yet") to instead assert the
  opposite: an insert with an order_id not present in `"order"` now fails
  with a `SQLException`
- New test verifying actual CASCADE behavior: insert an order, insert an
  order_item referencing it, delete the order, confirm the order_item row
  is also gone

# Non-Functional Requirements
- Migration applies cleanly on top of V1 + V2 + V3
- No Scala domain model/store changes (still schema-only)

# Acceptance Criteria
- [ ] New migration adds the FK constraint with `ON DELETE CASCADE`
- [ ] Inserting an order_item with an order_id not present in `"order"` now
  fails
- [ ] Deleting an order cascades to delete its order_items rows
- [ ] `sbt scalafmtCheck test` passes

# Out of Scope
- Scala domain model / store layer for order_items (deferred to US-3.1)
- Any HTTP API changes
