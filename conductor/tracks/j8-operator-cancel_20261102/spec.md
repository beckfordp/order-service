# Specification: Checkout Journey 8 — operator-initiated cancellation

## Overview

US-22 adds operator cancellation. No real auth/role model exists (user-service
issues opaque customer tokens; order-service trusts the request). This track
implements the agreed local/demo shortcut: a config-gated fixed operator token
in order-service, a typed cancellation actor on `OrderCancelled` (registry v3),
and a durable `order_cancellation_audit` row written in the same transaction as
the status change. Real operator identity is deferred and recorded in ADR 0024.
The gshop operator UI is deferred to ADM-7.

## Functional Requirements

- Config `operator-auth { token, operator-id }`, defaulting to the fixed local
  token `dev-operator-token` and id `dev-operator`, overridable via
  `ORDER_OPERATOR_TOKEN` / `ORDER_OPERATOR_ID`; a blank token disables the path.
- `PATCH`/`PUT /orders/{id}` accepts an optional `Authorization` header:
  - absent → customer cancellation exactly as today;
  - `Bearer <configured token>` → operator cancellation
    (`OrderEvent.OperatorCancelled(operatorId)`, `reason: operator`);
  - malformed or mismatched token → `401` before any read/write;
  - constant-time token comparison; never log the header/token.
- Operator eligibility is identical to customer cancellation: only
  `authorized`; dispatched/confirmed/cancelled → `409`.
- `OrderCancelledEvent` gains optional `actor: {type, id}` with type
  `customer | operator | system`; the producer always populates it:
  - customer PATCH → `{customer, order.customerId}`
  - expiry consumer → `{system, payment-service}`
  - operator PATCH → `{operator, configured id}`
- A new `order_cancellation_audit` table stores `(order_id, actor_type,
  actor_id, reason, created_at)` for every cancellation, in the same Postgres
  transaction as the status change and outbox row; `OrderStore` exposes a read
  method. In-memory writes it after the state update (test interpreter only).
- Add `Authorization` to the CORS allow-headers.
- ADR 0024, `system-design.md`, US-22 and registry v3 are updated; registry is
  exported and pushed before service repos.

## Acceptance Criteria

- Customer/system/operator cancellations produce the expected actor in the
  event, the audit row and logs; customer copy/behaviour is unchanged.
- Operator token accepted/rejected/absent tests; 401 persists nothing; 409 for
  ineligible statuses.
- Audit row rolls back with the transaction when the outbox write fails.
- `OrderCancelled` schema registers v3 with the optional actor; AsyncAPI docs
  include it (inline nested schema).
- `sbt scalafmtCheck test schemaRegistryCheck` green.

## Out of Scope

Real operator identity/roles (user-service), gshop operator UI (ADM-7),
customer authentication for the existing PATCH path.
