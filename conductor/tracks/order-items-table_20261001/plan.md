# Implementation Plan: Add order_items table

## Phase 1: Add order_items table

- [x] Task: Add Flyway migration V3__create_order_items.sql with the six specified columns (id, order_id, sku, product_name, unit_price_cents, quantity); extend MigrationsSuite with a test verifying the new table's columns and that the migration applies cleanly on top of V1+V2 `13bdda1`
- [ ] Task: Conductor - User Manual Verification 'Add order_items table' (Protocol in workflow.md)

Small, schema-only scope — kept as a single task since there's no
application code to separate out (no Scala layer in this track).
