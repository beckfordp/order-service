# Specification: Checkout Journey 7 — order-service race reconciliation

## Overview

`OrderCancelled` and the payment/fulfilment outcome events can arrive in any
order. Whichever transition order-service accepts first wins; a later
contradictory outcome is rejected by `OrderLifecycle`. Today those rejections
are only logged, so a dispatch that lost to a cancellation (or a capture after
a cancellation) leaves no manual-reconciliation record.

US-21.4 requires every side that loses the race to record a reconciliation
flag. Order-service already has `order_reconciliation` and
`OrderStore.flagReconciliation`; this track wires the loser branches to it and
fixes the in-memory interpreter's last-wins drift.

## Functional Requirements

- Any `IllegalTransition` from `OrderStore.transition` where `from != to` is a
  non-duplicate contradiction and must call
  `flagReconciliation(orderId, "<EventLabel> rejected from <from>")`:
  - `DispatchConfirmed` after `Cancelled` (and other contradictory states)
  - `PaymentCaptured` after `Cancelled` (and other contradictory states)
  - `PaymentFailed` after `Cancelled` (and other contradictory states)
  - `AuthorizationExpired` from `Dispatched` (shipped but not captured)
- `UnknownOrder` and `from == to` duplicates stay log-only (no flag).
- The flag write must not turn an expected loser into a DLQ retry: log the
  flag result, but commit the Kafka offset.
- In-memory `OrderStore.flagReconciliation` and the reconciliation write in
  `transition` become first-wins, matching Postgres `ON CONFLICT DO NOTHING`.
- No schema changes.

## Acceptance Criteria

- Consumer suites assert the reconciliation reason for each loser and no flag
  for duplicates/unknown orders.
- Store suites assert first-wins behaviour in both interpreters.
- A route-level test proves cancel-after-dispatch returns 409 (US-21.1).
- `sbt scalafmtCheck test` green.

## Out of Scope

Payment/fulfilment/inventory reconciliation stores (their own tracks), the
operator token/actor work (J8), gshop.
