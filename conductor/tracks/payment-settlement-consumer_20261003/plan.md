# Implementation Plan: US-6.3 consume payment.settled / payment.failed, update order status, publish order.status-changed

## Phase 1: OrderStatus extension + OrderStore.updateStatusIfCurrent [checkpoint: c24a466]

- [x] Task: Add OrderStatus.Confirmed ("confirmed") and OrderStatus.PaymentFailed ("payment_failed") cases to the ADT (asString/fromString) `8f3ac85`
- [x] Task: Write failing OrderStatus tests for the two new cases (asString + fromString round trip, plus fromString rejecting unrelated garbage still works) `8f3ac85`
- [x] Task: Add migration V6__add_payment_status_values.sql - drop+recreate order_status_check to include all 5 values `2b190cf`
- [x] Task: Write failing OrderStoreSuite (in-memory) + OrderStorePostgresSuite tests for a new `updateStatusIfCurrent(id, expected, newStatus): F[Option[UpdatedOrderRef]]` method: success when current==expected; no-op (None) when current!=expected; no-op (None) for unknown id `2b190cf`
- [x] Task: Implement updateStatusIfCurrent in the OrderStore trait + in-memory + Postgres impls (atomic guarded transition, mirrors updateStatusByItemId's pattern) `2b190cf`
- [x] Task: Run tests, confirm green `2b190cf` - 125 passed, 0 failed
- [x] Task: Conductor - User Manual Verification 'OrderStatus extension + updateStatusIfCurrent' (Protocol in workflow.md) `c24a466` - verified via committed script (prompt-off), see git note on checkpoint commit

## Phase 2: PaymentEventConsumer + Main wiring

- [ ] Task: Add PaymentSettledEvent/PaymentFailedEvent case classes (orderId, paymentId, amountCents, timestamp) with circe Codecs in a new PaymentEventConsumer.scala (mirrors StockEventConsumer.scala's co-located event mirrors)
- [ ] Task: Write a failing Testcontainers-Kafka test: a synthetic payment.settled event for a Reserved order moves it to confirmed and publishes order.status-changed (status="confirmed") with correct orderId/customerId, via a capturing test-double publisher
- [ ] Task: Write a failing test: a synthetic payment.failed event for a Reserved order moves it to payment_failed and publishes order.status-changed (status="payment_failed")
- [ ] Task: Write a failing test: an event for an order NOT currently Reserved (e.g. still pending, or already confirmed) is a no-op - no status change, no publish
- [ ] Task: Write a failing test: an event for an unknown orderId is a no-op, logged info, no crash
- [ ] Task: Write a failing test: a malformed/undecodable event is logged as a decode failure, offset still committed, no crash
- [ ] Task: Write a failing cross-topic negative test: settledStream never triggers a transition from a payment.failed-shaped event and vice versa
- [ ] Task: Implement PaymentEventConsumer (settledStream, failedStream merged via run[F]), using updateStatusIfCurrent + the existing OrderEventPublisher.publishStatusChanged
- [ ] Task: Wire PaymentEventConsumer.run into Main.scala as an additional backgrounded fiber alongside the existing StockEventConsumer, sharing the same OrderEventPublisher instance
- [ ] Task: Run tests, confirm green
- [ ] Task: Verify coverage (sbt coverage test coverageReport, target >80% on new code)
- [ ] Task: Conductor - User Manual Verification 'payment.settled/payment.failed consumer wiring' (final, Protocol in workflow.md)

Two phases - Phase 1 is the status/store layer in isolation (small,
self-contained schema+ADT change), Phase 2 is the consumer itself plus
wiring, proven end to end against synthetic Kafka events (no live
payment-service needed, same stub/fan-out pattern as every prior consumer
track).
