# Specification: US-18.3 — DispatchFailed → dispatch_failed + release

## Overview

order-service consumes `fulfilment-events: DispatchFailed`, moves an authorized
order to `dispatch_failed`, and releases its inventory reservations via the
existing idempotent release endpoint. No order-status event is published.

## Functional Requirements

- **FR-1 — Status.** `OrderStatus` gains `DispatchFailed`; migration `V11`
  widens the CHECK constraint.
- **FR-2 — Consumer.** `FulfilmentEventConsumer` also decodes
  `DispatchFailed {orderId, reason}`; unchanged null-safe/envelope/DLQ
  behaviour.
- **FR-3 — Transition.** Only an `authorized` order moves to `dispatch_failed`;
  unknown/other statuses are logged and skipped.
- **FR-4 — Release.** After a successful transition, call
  `InventoryClient.release(orderId)` (idempotent); failures are logged for
  reconciliation and do not roll back the status.
- **FR-5 — No event.** No `OrderStatusChanged` on dispatch failure.
- **FR-6 — AsyncAPI.** Subscribe `DispatchFailed` on `fulfilment-events`;
  schemaRegistryCheck passes.
- **FR-7 — Live proof.** Forced fulfilment failure → order `dispatch_failed`
  and reserved quantity restored; non-authorized orders unchanged.

## Non-Functional Requirements

- No new dependencies; reuse existing patterns; `sbt scalafmtCheck test` green.

## Acceptance Criteria

- Failure moves exactly one authorized order; duplicates no-op; inventory
  released; malformed payloads don't kill the stream; live check passes.

## Out of Scope

- payment void (US-18.2), notifications (US-18.4), gshop display (US-18.5),
  US-20 cancellation.
