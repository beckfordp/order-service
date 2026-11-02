# Plan: Checkout Journey 8 — operator-initiated cancellation

## Phase 1: Wire types and pure cancellation mapping

- [ ] Task: Failing tests for `OrderEvent.OperatorCancelled` lifecycle and the
  pure `OrderCancellation` actor/reason mapping (customer/operator/system).
- [ ] Task: Add `OrderActor`, `OperatorCancelled`, the `OrderCancelledEvent`
  actor field and `OrderCancellation`; update exhaustive matches (Green).
- [ ] Task: `sbt scalafmtCheck test`.

## Phase 2: Operator token and route

- [ ] Task: Failing config tests (`operator-auth` defaults/overrides/blank).
- [ ] Task: `OperatorAuthConfig`, `OperatorAuth` (constant-time Bearer
  comparison), `OrderError.Unauthorized` + 401 variant, Authorization header
  on PATCH/PUT, operator branch in `updateLogic`, CORS header (Green).
- [ ] Task: Route tests: absent → customer, valid → operator, invalid → 401
  and no transition, dispatched → 409; docs include header/401.
- [ ] Task: `sbt scalafmtCheck test`.

## Phase 3: Audit table and transaction

- [ ] Task: Failing store/Postgres tests: audit row for customer/system/
  operator; read method; rollback with the outbox failure (Red).
- [ ] Task: `V14__create_order_cancellation_audit.sql`, store audit insert in
  the Postgres transaction and in-memory interpreter, `cancellationAudit`
  read (Green).
- [ ] Task: Consumer/event tests: expiry actor `{system, payment-service}`;
  customer actor; operator actor; actor absent decodes to None.
- [ ] Task: `sbt scalafmtCheck test`.

## Phase 4: Registry and docs

- [ ] Task: `bin/registry-restore`; `sbt schemaRegistryCheck` (v2→v3);
  `bin/registry-export`; verify only `order-events-OrderCancelled` changed.
- [ ] Task: Update `system-design.md` (actor contract, Authorization/401,
  audit table) and add ADR 0024; commit/push gluon before service pushes.
- [ ] Task: Conductor - User Manual Verification 'Operator cancellation'
  (Protocol in workflow.md) — live operator PATCH + 401 + audit row.

## Review

- Filled after `/conductor:review`.
