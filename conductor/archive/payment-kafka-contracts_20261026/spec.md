# Spec: Rework 7 order-service — sync-payment-auth Kafka contracts

## Overview

Payment authorization is now synchronous, so order-service stops expecting authorization events
and instead consumes the capture/expiry outcomes. It also adopts the new `PaymentFailed` shape
(`reason`). Transactional behaviour stays minimal: `PaymentCaptured` → `Confirmed`,
`PaymentAuthorizationExpired` → the new `Cancelled` status; future statuses
(`Authorized`/`Dispatched`) and their journeys stay out of scope.

## Functional Requirements

1. **Payment-events consumptions**: remove `PaymentSettled`; add `PaymentCaptured` and
   `PaymentAuthorizationExpired`; keep `PaymentFailed` with the new payload. Never subscribe to
   `PaymentAuthorized` (learned synchronously).
2. **Local event mirrors** in `PaymentEventConsumer.scala`: delete `PaymentSettledEvent`; add
   `PaymentCapturedEvent {orderId, amountCents, timestamp}` and
   `PaymentAuthorizationExpiredEvent {orderId, timestamp}` (Codec + Schema); add `reason: String`
   to `PaymentFailedEvent`.
3. **Consumer dispatch**:
   - `PaymentCaptured` → `updateStatusIfCurrent(orderId, Reserved, Confirmed)` +
     `OrderStatusChanged {status: confirmed}` outbox row.
   - `PaymentAuthorizationExpired` → `updateStatusIfCurrent(orderId, Reserved, Cancelled)` +
     `OrderStatusChanged {status: cancelled}` outbox row.
   - `PaymentFailed` → existing behaviour (`Reserved` → `PaymentFailed`, outbox `payment_failed`)
     with the new payload shape.
   - Unknown `eventType` still silently skipped; guarded no-ops for wrong-status/unknown order
     remain.
4. **`OrderStatus`**: add `Cancelled` (`asString = "cancelled"`, `fromString` parses it). No other
   status changes.
5. **Migrations**: new migration extends the `order.status` CHECK constraint to include
   `cancelled` (keeping all existing values); `MigrationsSuite` proves accepted/rejected values.
6. **`OrderStatusChangedEvent`**: schema enum gains `cancelled`
   (`reservation_failed | confirmed | payment_failed | cancelled`).
7. **`AsyncApiChannels`**: exactly 7 channels — 2 inventory consumes, 3 payment consumes
   (`PaymentCaptured`/`PaymentAuthorizationExpired`/`PaymentFailed`), 2 order-events CDC
   publishes. `AsyncApiChannelsSuite` and `OrderAsyncApiDocsSuite` updated.
8. **Tests**: `PaymentEventConsumerSuite` replaces the PaymentSettled cases with PaymentCaptured
   and PaymentAuthorizationExpired cases (status + outbox assertions, wrong-state/unknown-id
   no-ops, new PaymentFailed shape); `OrderStatusSuite` and migration/outbox suites updated.
9. **Docs/scripts**: README and `conductor/product.md`/`tech-stack.md` event/consumer lists;
   update `verify-kafka-asyncapi-docs.sh`, `verify-payment-settlement-consumer-end-to-end.sh`
   (rename/retarget to capture/expiry), and `verify-schema-registry-check.sh` (artifacts + `gluon`
   group).
10. **`schemaRegistryCheck`** passes: `PaymentCaptured`/`PaymentAuthorizationExpired` register as
    new artifacts; `PaymentFailed` matches the new shape payment-service already registered;
    `PaymentSettled` is no longer checked.

## Non-Functional Requirements

- TDD; full suite green; no new dependencies.
- Guarded `updateStatusIfCurrent` transitions only — no unguarded writes.
- Preserve outbox/CDC, envelope, supervision/DLQ wiring unchanged.
- Exact payload field names/types per `system-design.md` (`amountCents`, `reason`, ISO-8601
  instants).

## Acceptance Criteria

- [ ] Channels exactly as in requirement 7; AsyncAPI docs contain the 3 payment consume types and
  neither `PaymentSettled` nor `PaymentAuthorized`.
- [ ] `PaymentCaptured` flips a `Reserved` order to `Confirmed` and writes an `OrderStatusChanged
  confirmed` outbox row.
- [ ] `PaymentAuthorizationExpired` flips a `Reserved` order to `Cancelled` and writes an
  `OrderStatusChanged cancelled` outbox row.
- [ ] `PaymentFailed` (with `reason`) still flips `Reserved` → `PaymentFailed` + outbox
  `payment_failed`.
- [ ] Migration CHECK accepts all five statuses incl. `cancelled` and rejects invalid ones.
- [ ] `sbt schemaRegistryCheck` passes against the local Apicurio `gluon` group; `sbt test` green.
- [ ] README/conductor docs and the three verification scripts no longer reference the retired
  `PaymentSettled` consumption.

## Out of Scope

- `DispatchConfirmed`/`DispatchFailed` consumers and `Dispatched`/`DispatchFailed` statuses
  (Journeys 3/4).
- `Authorized` status and the synchronous `PaymentClient` (Journey 1).
- Inventory release/notification on expiry (Journey 5), notification-service changes.
- Consuming `PaymentAuthorized`.
