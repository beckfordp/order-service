# Spec: Migrate to order-events/inventory-events/payment-events with envelope

## Overview
Migrate order-service's Kafka publish/consume from five per-event-type
topics (publishes `order.reserved`, `order.status-changed`; consumes
`inventory.stock-reserved`, `inventory.stock-reservation-failed`,
`payment.settled`, `payment.failed`) to three domain topics (`order-events`,
`inventory-events`, `payment-events`), every event wrapped in a shared
envelope carrying `eventType` so consumers dispatch on that instead of topic
subscription. order-service is the most central repo in this cutover — it
touches all three domain topics. Part of a coordinated cross-repo cutover
([ADR 0009](../../../../docs/adr/0009-one-kafka-topic-per-domain.md),
Accepted 2026-10-11; `gluon/PLAN.md`'s Rework 2) — inventory-service's
`StockEventPublisher` (done, `kafka-envelope_20261011`) and
payment-service's `OrderReservedConsumer`/`PaymentEventPublisher` (done,
`kafka-envelope_20261011`) already migrated their own sides.

## Functional Requirements
- New `EventEnvelope` case class (same shape as every other repo's copy,
  no shared library — matching this codebase's existing pattern):
  `eventId`, `eventType`, `timestamp`, `source`, `correlationId`,
  `payload: Json`.
- `OrderEventPublisher.publishReserved`/`publishStatusChanged` both produce
  to a single `order-events` topic (replacing `reservedTopic`/
  `statusChangedTopic`), wrapping the payload with `eventType =
  "OrderReserved"` / `"OrderStatusChanged"`. `source = "order-service"`,
  `correlationId = event.orderId`.
- `StockEventConsumer` subscribes to `inventory-events` (was
  `reservedTopic`/`reservationFailedTopic`, two topics merged via two
  separate consumer groups — collapses to one subscription, one consumer
  group). Decodes the envelope first; an `eventType` other than
  `StockReserved`/`StockReservationFailed` is silently skipped, not logged
  as malformed.
- `PaymentEventConsumer` subscribes to `payment-events` (was
  `settledTopic`/`failedTopic`, same collapse). Same envelope-decode-then-
  branch-on-eventType shape; an `eventType` other than
  `PaymentSettled`/`PaymentFailed` is silently skipped.
- Both consumers keep publishing via `OrderEventPublisher` exactly as
  before (now itself envelope/single-topic) on a successful transition.

## Non-Functional Requirements
- No new dependency.
- Existing retry/timeout behavior (`OrderEventPublisher`'s bounded retry)
  unchanged.
- Existing idempotent/no-op semantics (unknown id, already-resolved order)
  unchanged.

## Acceptance Criteria
- [x] `OrderEventPublisherSuite` confirms both event types land on
  `order-events`, decode as a valid `EventEnvelope`, payload round-trips.
- [x] `StockEventConsumerSuite` confirms: synthetic `StockReserved`/
  `StockReservationFailed` envelopes on `inventory-events` still drive
  order status + downstream `OrderReserved`/`OrderStatusChanged`
  publishes correctly; a differently-typed envelope on the same topic is
  ignored; existing no-op/malformed-event resilience behavior preserved.
- [x] `PaymentEventConsumerSuite` confirms the same shape for
  `PaymentSettled`/`PaymentFailed` on `payment-events`.
- [x] Full `sbt test` suite stays green (139/139).

## Out of Scope
- `purekafka`-based consumer resilience (Rework 3) — separate track.
- `system-design.md`'s "Kafka topics"/payload-contracts rewrite — done
  once all four repos in this cutover are complete, as its own final step.
