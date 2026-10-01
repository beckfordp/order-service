# Overview
Implement US-3.1: checkout creates an order. Extends `POST /orders` to accept
a list of line items (checkout's cart contents) and persist both the
`Order` and its `OrderItem`s atomically in a single Postgres transaction.
`totalCents` is now computed server-side from the items (quantity ×
unit_price_cents, summed), rather than client-supplied. `GET /orders/{id}`
and all order responses now include the item list. This closes out the
Scala domain model / store layer that the two earlier order_items tracks
(table + FK) deliberately deferred to this track.

# Functional Requirements
- New domain types: `OrderItem` (persisted: id, orderId, sku, productName,
  unitPriceCents, quantity) and `NewOrderItem` (input: sku, productName,
  unitPriceCents, quantity)
- `OrderStore.create(customerId, items: List[NewOrderItem]): F[(Order, List[OrderItem])]`
  replaces `create(customerId, totalCents)`. `totalCents` computed
  internally as `items.map(i => i.unitPriceCents * i.quantity).sum`.
  Postgres impl wraps the order insert + all order_item inserts in a single
  Skunk transaction (atomic)
- `OrderStore.get`/`update` signatures change to
  `F[Option[(Order, List[OrderItem])]]`, fetching items alongside the order
- HTTP: `CreateOrderRequest` becomes `{customerId, items: List[CreateOrderItemRequest]}`
  where `CreateOrderItemRequest = {sku, productName, unitPriceCents, quantity}`;
  `totalCents` is no longer a request field
- `OrderResponse` gains `items: List[OrderItemResponse]`
  (`{id, sku, productName, unitPriceCents, quantity}`), populated on
  create/get/patch/put
- New validation before calling the store: empty items list rejected
  (`EmptyOrderItems` → 400); any item with `quantity <= 0` or
  `unitPriceCents < 0` rejected (`InvalidOrderItem(reason)` → 400). Nothing
  persisted on rejection
- `createOrderEndpoint`'s error type changes from `Unit` to `OrderError`
  (can now fail validation), using the same `oneOf`/`oneOfVariant` pattern
  as other endpoints

# Non-Functional Requirements
- Order + items created atomically (one DB transaction) — no possibility of
  an order existing with partially-inserted items
- Existing endpoints' URLs/methods/status codes unchanged; only
  request/response bodies change
- Follows existing ADT/pure-FP conventions

# Acceptance Criteria
- [ ] POST /orders with a non-empty items list returns 201, totalCents
  computed from items, items echoed in response
- [ ] POST /orders with an empty items list returns 400, nothing persisted
- [ ] POST /orders with an item with quantity <= 0 or unitPriceCents < 0
  returns 400, nothing persisted
- [ ] An insert that fails partway through persisting items (e.g. a
  duplicate item id) rolls back the entire transaction — the order row is
  not left behind
- [ ] GET /orders/{id} returns the order's items
- [ ] PATCH/PUT /orders/{id} responses also include the (unchanged) items
  list
- [ ] DELETE /orders/{id} still cascades to remove order_items, verified
  end-to-end through the HTTP API
- [ ] `sbt scalafmtCheck test` passes

# Out of Scope
- Editing/adding/removing items on an existing order (immutable snapshot)
- Any call to inventory-service or catalog-service (deferred to US-4.2+)
- Redis caching (US-8.1)
