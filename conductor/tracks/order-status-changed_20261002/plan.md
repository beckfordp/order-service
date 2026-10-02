# Implementation Plan: US-5.4 publish order.status-changed (reservation_failed) when a reservation fails

## Phase 1: OrderStatusChangedEvent + OrderEventPublisher.publishStatusChanged [checkpoint: a212229]

- [x] Task: Add OrderStatusChangedEvent case class (orderId, customerId, status, timestamp) with a circe Codec to OrderEvents.scala `2a9e1c8`
- [x] Task: Write a failing Testcontainers-Kafka test: OrderEventPublisher.publishStatusChanged produces the event on the order.status-changed topic with the correct payload `2a9e1c8`
- [x] Task: Implement publishStatusChanged + statusChangedTopic constant on OrderEventPublisher, reusing the existing producer/bounded-retry plumbing built for publishReserved `2a9e1c8`
- [x] Task: Run tests, confirm green `2a9e1c8` - 3 passed, 0 failed
- [x] Task: Conductor - User Manual Verification 'OrderStatusChangedEvent + publishStatusChanged' (Protocol in workflow.md) `a212229` - verified via committed script (prompt-off), see git note on checkpoint commit

## Phase 2: Wire both failure paths (sync OrderRoutes + async StockEventConsumer) + Main

- [ ] Task: Write a failing OrderRoutesSuite test: a checkout whose synchronous reserve call fails results in order.status-changed published with status="reservation_failed" and the correct orderId/customerId (using a capturing test-double publisher, same pattern as existing historyCache/inventoryClient test doubles)
- [ ] Task: Add publisher: OrderEventPublisher[F] parameter to OrderRoutes.serverEndpoint; publish on the synchronous reservation-failure branch after a successful store.update, skipping on None (the known update-race edge case)
- [ ] Task: Add the same parameter to OrderRoutes.routes[F] (test-convenience aggregator), threading it to serverEndpoint
- [ ] Task: Update existing OrderRoutesSuite/HealthRoutesSuite call sites for the new signature
- [ ] Task: Write a failing Testcontainers-Kafka test in StockEventConsumerSuite: a synthetic inventory.stock-reservation-failed event against a matching Pending order results in both the status becoming ReservationFailed and order.status-changed published with the correct fields
- [ ] Task: Write a failing test confirming order.status-changed is never published from reservedStream (mirrors US-5.3's analogous negative test for order.reserved never publishing from reservationFailedStream)
- [ ] Task: Update StockEventConsumer.reservationFailedStream to publish on success (new publishStatusChangedIfUpdated helper, mirroring publishReservedIfUpdated); run[F] passes the publisher to both streams now
- [ ] Task: Wire orderEventPublisher into OrderRoutes.serverEndpoint's call site in Main.scala
- [ ] Task: Run tests, confirm green
- [ ] Task: Verify coverage (sbt coverage test coverageReport, target >80% on new code)
- [ ] Task: Conductor - User Manual Verification 'order.status-changed publish wiring' (final, Protocol in workflow.md)

Two phases - Phase 1 is the publish side in isolation (small, since it reuses US-5.3's producer/retry plumbing wholesale), Phase 2 wires both failure paths (the synchronous checkout path and the async consumer path both reach ReservationFailed, so both need the publish call) and proves each end to end.
