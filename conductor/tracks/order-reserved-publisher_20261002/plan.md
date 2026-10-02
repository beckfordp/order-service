# Implementation Plan: US-5.3 publish order.reserved once an order's stock is fully reserved

## Phase 1: OrderStore.updateStatusByItemId returns order details

- [ ] Task: Write a failing test (in-memory + Postgres via Testcontainers) asserting a successful transition returns the order's id/customerId/totalCents, not just true
- [ ] Task: Change the trait signature to F[Option[UpdatedOrderRef]] (new small case class: orderId, customerId, totalCents)
- [ ] Task: Update the Postgres impl's SQL RETURNING clause (id, customer_id, total_cents) and decode into UpdatedOrderRef
- [ ] Task: Update the in-memory impl to return the same shape
- [ ] Task: Update StockEventConsumer's two call sites for the new return type (reservedStream uses the returned fields; reservationFailedStream only needs updated-or-not, ignores the payload)
- [ ] Task: Update existing call sites in OrderStoreSuite, OrderStorePostgresSuite, StockEventConsumerSuite for the new signature
- [ ] Task: Run tests, confirm green
- [ ] Task: Conductor - User Manual Verification 'OrderStore.updateStatusByItemId returns order details' (Protocol in workflow.md)

## Phase 2: OrderReservedEvent + OrderEventPublisher

- [ ] Task: Add OrderReservedEvent case class (orderId, customerId, totalCents, timestamp) with a circe Codec
- [ ] Task: Write a failing Testcontainers-Kafka test: OrderEventPublisher.publish produces the event on the order.reserved topic with the correct payload
- [ ] Task: Implement OrderEventPublisher (fs2-kafka KafkaProducer[F,String,String], publish wrapped in purerest.resilience's bounded retry), mirroring StockEventPublisher's shape
- [ ] Task: Write a failing test: a producer that always fails causes the bounded retry to exhaust, logged loudly, no crash (mirrors StockEventPublisherSuite's publishFailed test)
- [ ] Task: Run tests, confirm green
- [ ] Task: Conductor - User Manual Verification 'Event payload + publisher' (Protocol in workflow.md)

## Phase 3: Wire the publish into StockEventConsumer + Main

- [ ] Task: Write a failing Testcontainers-Kafka test: a synthetic inventory.stock-reserved event against a matching Pending order results in both the status becoming Reserved and an order.reserved event published with the correct orderId/customerId/totalCents
- [ ] Task: Update StockEventConsumer.reservedStream to build and publish OrderReservedEvent on a successful transition, skipping publish on the existing no-op case
- [ ] Task: Wire OrderEventPublisher as a Resource in Main.scala and pass it into StockEventConsumer.run
- [ ] Task: Write a failing test confirming order.reserved is never published from reservationFailedStream (that's US-5.4's job, not this track's)
- [ ] Task: Run tests, confirm green
- [ ] Task: Verify coverage (sbt coverage test coverageReport, target >80% on new code)
- [ ] Task: Conductor - User Manual Verification 'order.reserved publish wiring' (final, Protocol in workflow.md)

Three phases, each independently testable before the next builds on it - Phase 1 is a pure data-shape change to existing code, Phase 2 is the publish side in isolation (same pattern as inventory-service's US-5.1 and payment-service's just-landed PaymentEventPublisher), Phase 3 wires them together and proves the end-to-end outcome system-design.md already pinned.
