# Plan: checkout-journey1_20261027

## Phase 1: Domain + persistence
- [ ] Task: `Address` type + validation (US-11.1)
- [ ] Task: V9 migration (`shipping_address` jsonb, `authorized` in CHECK) + `OrderStatus.Authorized`
- [ ] Task: `OrderStore.createAuthorized` (in-memory + postgres; pre-minted ids, address, one tx) + `shippingAddress` read back by get/list
- [ ] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md)

## Phase 2: Outbound clients
- [ ] Task: `InventoryClient.reserveOrder` / `release` (stub Client[F] tests: 201/409/other; 204/other); per-item `reserve` dropped with the handler switch in Phase 3
- [ ] Task: `PaymentClient[F]` + `payment-client` config/`PAYMENT_SERVICE_URL` (stub tests: 200/402/404/other; config suite)
- [ ] Task: Wire PaymentClient in Main/AppRoutes
- [ ] Task: Conductor - User Manual Verification 'Phase 2' (Protocol in workflow.md)

## Phase 3: Checkout flow
- [ ] Task: New `OrderError` variants + error outputs (400/402/409/503×2/500)
- [ ] Task: Reshape `POST /orders` handler (validate → mint → reserve → authorize → persist; release on post-reserve failure; release failure logged only) — route tests per status, assert no row + release calls
- [ ] Task: `PaymentEventConsumer` expects `authorized`
- [ ] Task: Conductor - User Manual Verification 'Phase 3' (Protocol in workflow.md)

## Phase 4: Retire legacy + verify
- [ ] Task: Remove `StockEventConsumer` (+ KafkaConsumers wiring, Subscribe channels, tests), `OrderReservedEvent` channel, now-unused store methods; `OrderStatusChanged` enum drops `reservation_failed`; `schemaRegistryCheck`; delete `order-events-OrderReserved` artifact
- [ ] Task: Delete/replace verify scripts for removed behaviour (stock consumer, order-reserved e2e, orderitemid wiring, reserve resilience, checkout-creates-order)
- [ ] Task: `scripts/verify-checkout-journey1.sh` (real service + Postgres, Python stand-ins for inventory/payment): authorized, declined, out-of-stock, unknown method, release calls observed
- [ ] Task: Conductor - User Manual Verification 'Phase 4' (Protocol in workflow.md)
