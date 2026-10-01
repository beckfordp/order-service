# Implementation Plan: Add FK constraint on order_items.order_id

## Phase 1: Add FK constraint on order_items.order_id [checkpoint: b669908]

- [x] Task: Add Flyway migration V4__add_order_items_fk.sql (FK + ON DELETE CASCADE); update the prior track's 'no FK yet' MigrationsSuite test to assert rejection instead, and add a new test verifying actual CASCADE-delete behavior `63a8d52`
- [x] Task: Conductor - User Manual Verification 'Add FK constraint on order_items.order_id' (Protocol in workflow.md) `b669908`

Small, schema-only scope — kept as a single task, consistent with the
previous order_items track.
