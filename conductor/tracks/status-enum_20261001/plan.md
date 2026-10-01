# Implementation Plan: Harden Order.status to OrderStatus ADT

## Phase 1: Harden Order.status to OrderStatus ADT [checkpoint: 715ff12]

- [x] Task: Add `OrderStatus` sealed trait (Pending/Reserved/ReservationFailed) with `fromString`/`asString` + unit tests `bfa0970`
- [x] Task: Add `InvalidStatus(raw)` case to `OrderError`; wire a new errorOut mapping in OrderRoutes (alongside the existing notFoundOutput) `189c82c`
- [x] Task: Add Flyway migration `V2__add_order_status_check.sql` (CHECK constraint on status); verify it applies cleanly on top of V1 `f52b038`
- [x] Task: Add Skunk `orderStatus` eimap codec; update `Order`, `OrderStore` (in-memory + postgres) to use `OrderStatus` instead of raw String `189c82c`
- [x] Task: Update `OrderRoutes` PATCH/PUT to parse the request's status via `OrderStatus.fromString`, short-circuiting invalid values to `InvalidStatus`; `OrderResponse` keeps serializing status as a String via `asString` `189c82c`
- [x] Task: Add/extend tests — invalid-status PATCH/PUT returns 400 with nothing persisted; valid-status PATCH/PUT behaves exactly as before; OrderStatus.fromString/asString round-trips `189c82c`
- [x] Task: Conductor - User Manual Verification 'Harden Order.status to OrderStatus ADT' (Protocol in workflow.md) `715ff12`

Each task follows the standard TDD lifecycle from workflow.md (failing test
→ implement → refactor → commit → git note) during `/conductor:implement`.
Kept as a single phase since this is one cohesive unit of work.

## Phase: Review Fixes

- [x] Task: Apply review suggestions `6f39291`
