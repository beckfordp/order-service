# Specification: US-6.3 consume payment.settled / payment.failed, update order status, publish order.status-changed

## Overview
order-service consumes `payment.settled`/`payment.failed` from Kafka
(payment-service, US-6.1) and transitions the matching order from `reserved`
to `confirmed`/`payment_failed`, publishing `order.status-changed` for the
new status - closing Phase 4's consumer side. Mirrors `StockEventConsumer`'s
two-stream-merge pattern exactly: fs2-kafka, plain String key/value,
hand-rolled circe JSON mirrors of the already-pinned payload contract
(`gluon/docs/system-design.md`), Testcontainers Kafka for tests.

## Functional Requirements
- New `OrderStatus` cases: `Confirmed` ("confirmed"), `PaymentFailed`
  ("payment_failed").
- New migration `V6__add_payment_status_values.sql`: drop+recreate the
  `order_status_check` CHECK constraint to include the two new values.
- New `OrderStore` method `updateStatusIfCurrent(id, expected, newStatus):
  F[Option[UpdatedOrderRef]]` (in-memory + Postgres) - atomic guarded
  transition, only applies if current status == expected (`Reserved`); `None`
  on not-found or wrong-current-status (idempotent no-op). Reuses the
  existing `UpdatedOrderRef` projection.
- New `PaymentSettledEvent`/`PaymentFailedEvent` local mirrors (`orderId`,
  `paymentId`, `amountCents`, `timestamp`), circe `Codec`.
- New `PaymentEventConsumer`: `settledStream` (group
  `order-service-payment-settled`, topic `payment.settled`, target
  `Confirmed`) and `failedStream` (group `order-service-payment-failed`,
  topic `payment.failed`, target `PaymentFailed`), each: decode -> on
  failure log+commit-skip; on success call `updateStatusIfCurrent`, publish
  `order.status-changed` via the existing `OrderEventPublisher` on `Some`,
  log outcome, commit offset regardless. `run[F]` merges both streams.
- `Main.scala`: wire `PaymentEventConsumer.run` as an additional
  backgrounded fiber alongside the existing `StockEventConsumer`, sharing
  the same `OrderEventPublisher` instance.

## Non-Functional Requirements
- No `OrderEventPublisher` changes - consumer-only track, reuses
  `publishStatusChanged` as-is.
- Decode failures and no-op cases logged info/error per existing
  `StockEventConsumer` precedent - never crash the stream.
- No new idempotency guard beyond the status-equality check; a redelivered
  event for an already-`Confirmed` order is a safe no-op. payment-service's
  own double-charge prevention is US-6.2, separate.
- Scalafmt-clean; existing tagless-final/ADT-error conventions.

## Acceptance Criteria
- Synthetic `payment.settled` for a `Reserved` order -> `confirmed` +
  `order.status-changed` published (`status: "confirmed"`).
- Synthetic `payment.failed` for a `Reserved` order -> `payment_failed` +
  `order.status-changed` published.
- Event for an order NOT currently `Reserved` -> no-op, no publish, logged
  info.
- Event for an unknown `orderId` -> no-op, logged info, no crash.
- Malformed event -> logged decode failure, offset still committed, no
  crash.
- `sbt scalafmtCheck test` passes, coverage bar maintained.
- No cross-repo doc changes needed from this track -
  `gluon/docs/user-stories.md` and `PLAN.md` already have `US-6.3` and the
  payload contract pinned.

## Out of Scope
- US-6.2 (payment-service Redis idempotency) - separate repo/backlog item.
- US-7.1 (notification-service) - separate repo/track.
- US-9/US-10 (cancel-order, stale-pending recovery) - deferred epics.
- Any change to `OrderEventPublisher`'s publish side/retry policy/topic
  constants - reused unchanged from US-5.3/US-5.4.
