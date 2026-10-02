# Specification: US-5.4 publish order.status-changed (reservation_failed) when a reservation fails

## Overview
When an order reaches ReservationFailed - whether from the synchronous checkout-failure path (OrderRoutes, when reserveAll fails) or the async path (StockEventConsumer.reservationFailedStream, consuming inventory.stock-reservation-failed) - order-service publishes order.status-changed so notification-service (US-7.1) can email the customer. Closes a gap found 2026-10-02: system-design.md decided order-service publishes this, but no task was ever assigned.

## Functional Requirements
- New OrderStatusChangedEvent case class (orderId, customerId, status, timestamp) in OrderEvents.scala, matching system-design.md's pinned order.status-changed payload.
- Extend OrderEventPublisher (not a new object) with a second method, publishStatusChanged(event: OrderStatusChangedEvent): F[Unit], and a statusChangedTopic = "order.status-changed" constant - same producer, same bounded-retry/log-and-drop wrapper already built for publishReserved.
- OrderRoutes.serverEndpoint gains a new publisher: OrderEventPublisher[F] parameter. On the synchronous reservation-failure branch, after store.update(order.id, ReservationFailed) succeeds, publish OrderStatusChangedEvent(orderId, customerId, "reservation_failed", now). Skip the publish if update returns None (the known, already-backlogged update-race edge case - not this track's job to fix).
- OrderRoutes.routes[F] (the test-convenience aggregator) also gains the publisher parameter, threading it to serverEndpoint.
- StockEventConsumer.reservationFailedStream gains the publisher parameter too. After a successful updateStatusByItemId transition to ReservationFailed, publish the same event shape, skipped on the existing no-op case (unknown item id / already resolved) - mirroring reservedStream's publishReservedIfUpdated pattern from US-5.3.
- Main.scala: pass the already-constructed orderEventPublisher into both OrderRoutes.serverEndpoint and StockEventConsumer.run's reservationFailedStream call site.

## Non-Functional Requirements
- Same reliability stance as order.reserved: no transactional outbox, bounded retry via cats-retry, log loudly and drop on exhaustion.
- No duplicate-publish guard - if a reservation fails twice (shouldn't happen given Pending is a one-way gate, but not newly introduced risk either way).

## Acceptance Criteria
- A checkout whose synchronous reserve call fails results in both an order status of reservation_failed AND an order.status-changed event published with the correct orderId/customerId/status="reservation_failed".
- A synthetic inventory.stock-reservation-failed event against a matching Pending order results in both the status becoming ReservationFailed AND the same event published.
- No match (unknown orderItemId, or already resolved) publishes nothing, on both paths.
- order.status-changed is never published from reservedStream (that path publishes order.reserved only, per US-5.3).
- publishStatusChanged's wire format is correct (happy-path test); the bounded-retry/log-and-drop mechanism itself is not re-tested here - already proven generically by OrderEventPublisherSuite's existing forced-failure test against publishReserved (same shared retry code).

## Out of Scope
- order.status-changed for confirmed / payment_failed (US-6.3, separate track, triggered by consuming payment outcomes - not reservation failure).
- Fixing the known update-race edge case where OrderStore.update returns None (separate backlog "fix" item).
- notification-service's consumption of this event (US-7.1, separate track, separate repo not yet onboarded).
