# Implementation Plan: US-3.1 checkout creates an order

**Deviation note (mid-implementation):** The planned Phase 1 / Phase 2 split
assumed the store layer could reach a compiling, green, checkpointable state
before the HTTP layer was touched. That's not achievable: `OrderStore.create`
replacing `totalCents` with `items` (per the earlier "Store API" decision)
means `OrderRoutes.scala`'s call site breaks immediately — Scala compiles the
whole module at once, so there's no way to commit a passing Phase 1 without
also adapting the HTTP layer's create path. The phase headings below stay as
organizational labels, but there is only **one** manual-verification
checkpoint, at the true end, not one per phase — same situation as the
status-enum track's task-combining, just spanning what was planned as two
phases this time.

## Phase 1: Domain model and transactional store

- [x] Task: Add OrderItem and NewOrderItem domain types `1978853`
- [x] Task: Add Skunk codecs/queries for order_items (insert, select-by-order-id); update OrderStore.create (in-memory + postgres) to accept items: List[NewOrderItem], compute totalCents from items, persist order+items atomically (Postgres: single transaction wrapping both inserts) `1978853`
- [x] Task: Update OrderStore.get and OrderStore.update (in-memory + postgres) to return (Order, List[OrderItem]), fetching items alongside the order `1978853`
- [x] Task: Update existing test call sites (OrderStoreSuite, OrderStorePostgresSuite, OrderRoutesSuite's failingStore, HealthRoutesSuite stubs) to the new signatures; add new tests for totalCents computed correctly, items round-trip through create+get, transaction rollback on a failed item insert, CASCADE delete removes items (store-level) `1978853`
- [x] Task: (added mid-implementation, see note) Add Flyway migration V5__add_order_items_checks.sql (CHECK quantity > 0, CHECK unit_price_cents >= 0) — defense-in-depth needed to make the transaction-rollback test possible, since order_items had no constraint a valid NewOrderItem could violate `1978853`

## Phase 2: HTTP layer wiring

- [x] Task: Add CreateOrderItemRequest, update CreateOrderRequest (drop totalCents, add items), add OrderItemResponse, update OrderResponse to include items `1978853`
- [x] Task: Add EmptyOrderItems and InvalidOrderItem(reason) to OrderError; add their errorOut variants; change createOrderEndpoint's error type from Unit to OrderError `1978853`
- [x] Task: Update OrderRoutes server logic (create validates items before calling the store; get/update/delete adapted to the new store return shapes and OrderResponse construction) `1978853`
- [x] Task: Update/extend OrderRoutesSuite for the new request/response shapes (POST with items happy path, empty-items 400, invalid-item 400, GET/PATCH/PUT responses include items, DELETE still cascades end-to-end via HTTP) `1978853`
- [x] Task: Conductor - User Manual Verification 'US-3.1 checkout creates an order' (Protocol in workflow.md) `fbe0df7`

Two phases since the store layer can be fully implemented and verified
independently (via OrderStoreSuite/OrderStorePostgresSuite) before the HTTP
layer is wired on top of it. Each task follows the standard TDD lifecycle
from workflow.md.
