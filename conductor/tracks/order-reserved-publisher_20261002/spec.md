# Specification: US-5.3 publish order.reserved once an order's stock is fully reserved

## Overview
Once StockEventConsumer (US-5.2) successfully transitions an order from Pending to Reserved, order-service publishes order.reserved so payment-service (US-6.1) can charge it. This is order-service's first Kafka producer - it has been consumer-only so far.

## Functional Requirements
- Extend OrderStore.updateStatusByItemId (trait + Postgres + in-memory impls) to return the order's id/customerId/totalCents atomically on a successful transition, instead of just a Boolean - one RETURNING query, no second round-trip.
- New OrderReservedEvent case class (local mirror of system-design.md's pinned payload): orderId, customerId, totalCents, timestamp (ISO-8601). Circe Codec via deriveCodec.
- New OrderEventPublisher object mirroring inventory-service's StockEventPublisher: wraps a fs2-kafka KafkaProducer[F,String,String], publish wrapped in purerest.resilience's bounded retry, log-and-drop on exhaustion (no outbox).
- StockEventConsumer.reservedStream: on a successful transition, build OrderReservedEvent from the returned fields + current timestamp and publish it via OrderEventPublisher. Skip publish entirely on the existing idempotent no-op case (no match / already resolved).
- New `order.reserved` topic constant, same style as the existing reservedTopic/reservationFailedTopic vals.
- Wire OrderEventPublisher as a Resource in Main.scala alongside the existing Kafka consumer wiring.
- reservationFailedStream is unchanged - order.reserved is never published on reservation failure.

## Non-Functional Requirements
- No transactional outbox - reuses purerest.resilience's bounded retry; final-failure is logged loudly and dropped, matching the already-accepted risk level for inventory-service's own publisher.
- The consumed inventory.stock-reserved event's Kafka offset is still committed regardless of the order.reserved publish outcome (matches today's behavior for decode failures).

## Acceptance Criteria
- A synthetic inventory.stock-reserved event (Testcontainers-Kafka) against a matching Pending order results in both the status becoming Reserved AND an order.reserved event being published with the correct orderId/customerId/totalCents.
- No match (unknown orderItemId, or already resolved) publishes nothing.
- A forced publish failure (producer stub that always fails, bounded retry exhausted) is logged loudly, doesn't crash the consumer stream, and the stock-reserved offset is still committed.
- order.reserved is never published from the reservation-failed path.

## Out of Scope
- order.status-changed publishing for reservation_failed, confirmed, or payment_failed - separate tracks (US-5.4, US-6.3).
- Idempotency/dedup on order.reserved itself (no consumer exists yet; payment-service's own idempotency, US-6.2, covers double-delivery on its side).
- Any change to the synchronous checkout/reserve path (US-3.1/US-4.2).
