# Plan: Checkout Journey 8 — operator-initiated cancellation

## Phase 1: Wire types and pure cancellation mapping

- [x] Task: Failing tests for `OrderEvent.OperatorCancelled` lifecycle and the
  pure `OrderCancellation` actor/reason mapping (customer/operator/system).
- [x] Task: Add `OrderActor`, `OperatorCancelled`, the `OrderCancelledEvent`
  actor field and `OrderCancellation`; update exhaustive matches (Green). (badfe73)
- [x] Task: `sbt scalafmtCheck test`.

## Phase 2: Operator token and route

- [x] Task: Failing config tests (`operator-auth` defaults/overrides/blank).
- [x] Task: `OperatorAuthConfig`, `OperatorAuth` (constant-time Bearer
  comparison), `OrderError.Unauthorized` + 401 variant, Authorization header
  on PATCH/PUT, operator branch in `updateLogic`, CORS header (Green). (badfe73)
- [x] Task: Route tests: absent → customer, valid → operator, invalid → 401
  and no transition, dispatched → 409; docs include header/401.
- [x] Task: `sbt scalafmtCheck test`.

## Phase 3: Audit table and transaction

- [x] Task: Failing store/Postgres tests: audit row for customer/system/
  operator; read method; rollback with the outbox failure (Red). (badfe73)
- [x] Task: `V14__create_order_cancellation_audit.sql`, store audit insert in
  the Postgres transaction and in-memory interpreter, `cancellationAudit`
  read (Green). (badfe73)
- [x] Task: Consumer/event tests: expiry actor `{system, payment-service}`;
  customer actor; operator actor; actor absent decodes to None.
- [x] Task: US-9.7 — repeat cancel returns 200 with one event; customer cancel
  racing `PaymentAuthorizationExpired` produces one transition/event.
- [x] Task: US-9.6 — test that a failed inventory release after
  `DispatchFailed` records the reconciliation flag.
- [x] Task: `sbt scalafmtCheck test`.

## Phase 4: Registry and docs

- [x] Task: `bin/registry-restore`; `sbt schemaRegistryCheck` (v2→v3);
  `bin/registry-export`; verify only `order-events-OrderCancelled` changed. (486f1db)
- [x] Task: Update `system-design.md` (actor contract, Authorization/401,
  audit table) and add ADR 0024; commit/push gluon before service pushes. (486f1db)
- [x] Task: Conductor - User Manual Verification 'Operator cancellation'
  (Protocol in workflow.md) — verified by route/Postgres suites (valid token,
  401, audit row); the cross-service live operator check runs at journey
  verification after the dependent tracks land.

## Review

- Reviewed 2026-11-02: plan compliance yes; style pass; new tests cover the
  operator token (accepted/rejected/absent), audit for all three actors,
  same-transaction rollback, repeat-cancel idempotency, cancel-vs-expiry
  single winner and the US-9.6 release-failure flag. Full suite 220 passed.
  Accepted caveat: the fixed dev token is a local/demo actor selector, not an
  authorization boundary (ADR 0024). No blocking findings.
