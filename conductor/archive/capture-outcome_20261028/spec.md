# Specification: US-19.2 + US-19.3 — capture outcome

## Overview

order-service consumes payment-service's asynchronous capture outcome from
`payment-events`:
- `PaymentCaptured` -> order status `Confirmed` (US-19.2).
- `PaymentFailed {reason: capture_failed}` -> order status `PaymentFailed`, a
  durable manual-reconciliation flag, and **no** automatic inventory release —
  the goods may already be physically shipped (US-19.3).

Both are guarded on an order awaiting capture: `authorized` (the outcome
overtook `DispatchConfirmed`) or `dispatched` (the usual case). The two
consumers are independent streams, so both source states must be accepted to
avoid silently dropping an outcome.

## Functional Requirements

- **FR-1 — Reconciliation persistence.** Migration `V12` adds
  `order_reconciliation (order_id PK FK->order ON DELETE CASCADE, reason,
  created_at)`. `OrderStore` gains `recordCaptureOutcome(id, newStatus,
  outboxEvent, reconciliationReason)` doing the conditional
  `authorized|dispatched -> new` update, the `OrderStatusChanged` outbox row,
  and the reconciliation row in one transaction; plus
  `reconciliationReason(id): F[Option[String]]`.
- **FR-2 — Participants.** Both interpreters: in-memory (`Ref` map) and
  Postgres (parameterised two-status guard, `ON CONFLICT DO NOTHING` for the
  flag, conditional select).
- **FR-3 — Consumer.** `PaymentEventConsumer` handles `PaymentCaptured` ->
  `Confirmed` and `PaymentFailed` -> `PaymentFailed` with
  `reconciliationReason = Some(event.reason)`; neither path has an
  `InventoryClient` dependency (structural no-auto-release). Expiry
  (`Authorized -> Cancelled`) is unchanged.
- **FR-4 — Idempotency.** A redelivered outcome or an order not awaiting
  capture (`pending`/`confirmed`/`payment_failed`/...) is a logged no-op with
  no outbox or reconciliation row.
- **FR-5 — Outbox.** Successful transitions publish
  `OrderStatusChanged {status: confirmed|payment_failed}` as today.
- **FR-6 — Live proof.** `scripts/verify-capture-outcome.sh` proves the
  success path, duplicate injection, the decline path (flag set, inventory
  reserved quantity unchanged).

## Non-Functional Requirements

- No new dependencies; `sbt scalafmtCheck test` and `schemaRegistryCheck`
  green (AsyncAPI channels unchanged); >80% coverage of new code.

## Acceptance Criteria

- Dispatched (or racing Authorized) order -> `confirmed` on capture; capture
  failure -> `payment_failed` + reconciliation row; no inventory release;
  duplicate events harmless; live script passes.

## Out of Scope

gshop US-19.5, notification-service US-19.4, Journey 5 expiry sweep, changing
dispatch handlers, HTTP exposure of the reconciliation flag.
