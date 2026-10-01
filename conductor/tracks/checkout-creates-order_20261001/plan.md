# Implementation Plan: US-3.1 checkout creates an order

## Phase 1: Domain model and transactional store

- [ ] Task: Add OrderItem and NewOrderItem domain types
- [ ] Task: Add Skunk codecs/queries for order_items (insert, select-by-order-id); update OrderStore.create (in-memory + postgres) to accept items: List[NewOrderItem], compute totalCents from items, persist order+items atomically (Postgres: single transaction wrapping both inserts)
- [ ] Task: Update OrderStore.get and OrderStore.update (in-memory + postgres) to return (Order, List[OrderItem]), fetching items alongside the order
- [ ] Task: Update existing test call sites (OrderStoreSuite, OrderStorePostgresSuite, OrderRoutesSuite's failingStore, HealthRoutesSuite stubs) to the new signatures; add new tests for totalCents computed correctly, items round-trip through create+get, transaction rollback on a failed item insert, CASCADE delete removes items (store-level)
- [ ] Task: Conductor - User Manual Verification 'Domain model and transactional store' (Protocol in workflow.md)

## Phase 2: HTTP layer wiring

- [ ] Task: Add CreateOrderItemRequest, update CreateOrderRequest (drop totalCents, add items), add OrderItemResponse, update OrderResponse to include items
- [ ] Task: Add EmptyOrderItems and InvalidOrderItem(reason) to OrderError; add their errorOut variants; change createOrderEndpoint's error type from Unit to OrderError
- [ ] Task: Update OrderRoutes server logic (create validates items before calling the store; get/update/delete adapted to the new store return shapes and OrderResponse construction)
- [ ] Task: Update/extend OrderRoutesSuite for the new request/response shapes (POST with items happy path, empty-items 400, invalid-item 400, GET/PATCH/PUT responses include items, DELETE still cascades end-to-end via HTTP)
- [ ] Task: Conductor - User Manual Verification 'HTTP layer wiring' (Protocol in workflow.md)

Two phases since the store layer can be fully implemented and verified
independently (via OrderStoreSuite/OrderStorePostgresSuite) before the HTTP
layer is wired on top of it. Each task follows the standard TDD lifecycle
from workflow.md.
