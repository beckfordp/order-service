# Plan: checkout-journey1_20261027

## Phase 1: Domain + persistence [checkpoint: dbaa89b]
- [x] Task: `Address` type + validation (US-11.1) `0c1586c`
- [x] Task: V9 migration (`shipping_address` jsonb, `authorized` in CHECK) + `OrderStatus.Authorized` `bcf27dd`
- [x] Task: `OrderStore.createAuthorized` (in-memory + postgres; pre-minted ids, address, one tx) + `shippingAddress` read back by get/list `111d368`
- [x] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md) — prompt-off: 179 tests pass, coverage 91.79% stmt / 91.47% branch; store-only phase, end-to-end script in Phase 4

## Phase 2: Outbound clients [checkpoint: 537edfe]
- [x] Task: `InventoryClient.reserveOrder` / `release` (stub Client[F] tests: 201/409/other; 204/other); per-item `reserve` dropped with the handler switch in Phase 3 `ff6f005`
- [x] Task: `PaymentClient[F]` + `payment-client` config/`PAYMENT_SERVICE_URL` (stub tests: 200/402/404/other; config suite) `c3c1f48`
- [x] Task: Wire PaymentClient in Main/AppRoutes — folded into the Phase 3 handler task (the handler is its first consumer, so wiring earlier would mean an unused parameter threaded through every route test twice)
- [x] Task: Conductor - User Manual Verification 'Phase 2' (Protocol in workflow.md) — prompt-off: 189 tests pass, coverage 91.84% stmt / 92.14% branch; clients verified against stub Client[F]s, live paths covered by Phase 4 script

## Phase 3: Checkout flow [checkpoint: f1f9442]
- [x] Task: New `OrderError` variants + error outputs (400/402/409/503×2/500) — landed with the handler task below (outputs are only testable through it)
- [x] Task: Reshape `POST /orders` handler (validate → mint → reserve → authorize → persist; release on post-reserve failure; release failure logged only) — route tests per status, assert no row + release calls `ee91131`
- [x] Task: `PaymentEventConsumer` expects `authorized` `8631e54`
- [x] Task: Conductor - User Manual Verification 'Phase 3' (Protocol in workflow.md) — prompt-off: 188 tests pass, coverage 91.74% stmt / 91.04% branch (OrderRoutes 98.1%, PaymentClient 97.5%, InventoryClient/Address 100%); live end-to-end in Phase 4 script

## Phase 4: Retire legacy + verify
- [x] Task: Remove `StockEventConsumer` (+ KafkaConsumers wiring, Subscribe channels, tests), `OrderReservedEvent` channel, now-unused store methods; `OrderStatusChanged` enum drops `reservation_failed`; `schemaRegistryCheck`; delete `order-events-OrderReserved` artifact `b852989`
- [ ] Task: Delete/replace verify scripts for removed behaviour (stock consumer, order-reserved e2e, orderitemid wiring, reserve resilience, checkout-creates-order)
- [ ] Task: `scripts/verify-checkout-journey1.sh` (real service + Postgres, Python stand-ins for inventory/payment): authorized, declined, out-of-stock, unknown method, release calls observed
- [ ] Task: Conductor - User Manual Verification 'Phase 4' (Protocol in workflow.md)
