# Implementation Plan: Harden Order.status to OrderStatus ADT

## Phase 1: Harden Order.status to OrderStatus ADT

- [x] Task: Add `OrderStatus` sealed trait (Pending/Reserved/ReservationFailed) with `fromString`/`asString` + unit tests `bfa0970`
- [ ] Task: Add `InvalidStatus(raw)` case to `OrderError`; wire a new errorOut mapping in OrderRoutes (alongside the existing notFoundOutput) — combined with the two tasks below into one commit (see note at bottom): can't compile/test independently once OrderStore's signature changes
- [x] Task: Add Flyway migration `V2__add_order_status_check.sql` (CHECK constraint on status); verify it applies cleanly on top of V1 `f52b038`
- [ ] Task: Add Skunk `orderStatus` eimap codec; update `Order`, `OrderStore` (in-memory + postgres) to use `OrderStatus` instead of raw String
- [ ] Task: Update `OrderRoutes` PATCH/PUT to parse the request's status via `OrderStatus.fromString`, short-circuiting invalid values to `InvalidStatus`; `OrderResponse` keeps serializing status as a String via `asString`
- [ ] Task: Add/extend tests — invalid-status PATCH/PUT returns 400 with nothing persisted; valid-status PATCH/PUT behaves exactly as before; OrderStatus.fromString/asString round-trips
- [ ] Task: Conductor - User Manual Verification 'Harden Order.status to OrderStatus ADT' (Protocol in workflow.md)

Each task follows the standard TDD lifecycle from workflow.md (failing test
→ implement → refactor → commit → git note) during `/conductor:implement`.
Kept as a single phase since this is one cohesive unit of work.
