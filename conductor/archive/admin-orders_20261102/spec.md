# Specification: ADM-4 — admin order list + reconciliation workflow

## Overview

The Admin console needs every order (not just a customer's history) and a
queue of open reconciliation flags with an auditable resolve action. This
track adds order-service's admin-wide list plus its own reconciliation
open/resolved workflow, as the reference implementation for the three other
services (ADR 0026).

## Functional Requirements

### 1. Admin order list
- `GET /admin/orders?status=&customerId=` — both filters optional and ANDed,
  blank = absent; unknown `status` → `400` (`InvalidStatus`).
- `200`: `List[OrderResponse]`, newest first (created_at desc, id tie-break),
  directly from Postgres; never reads or writes `OrderHistoryCache`.
- No pagination (demo scale); unauthenticated local/demo read.
- Full tapir docs, registered in `AppRoutes`; `OrderDocsSuite` updated.

### 2. Reconciliation persistence
- `V15__extend_order_reconciliation.sql`: add `status text not null default
  'open'`, `operator_id text`, `note text`, `resolved_at timestamptz`; named
  status check; consistency check (`resolved` requires operator_id +
  resolved_at, `open` requires both null); index `(status, created_at desc)`.
- Existing rows become `open`; the first-wins insert path is unchanged.

### 3. Queue and resolve
- `GET /reconciliations?status=&orderId=` — newest first; blank filters are
  absent; unknown status `400`.
- `PATCH /reconciliations/{orderId}`:
  - requires a valid dev operator token (`Authorization: Bearer`) — absent,
    malformed or mismatched → `401`;
  - body `{status: "resolved", note: optional}`; any other status → `400`;
  - conditional `UPDATE ... WHERE status='open'` plus a same-session read →
    `Resolved | AlreadyResolved | NotFound`; unknown → `404`; repeat →
    idempotent `200` with the original operator/note/timestamp unchanged;
  - records operator id (from config) and `resolved_at` from DB `now()`.
- Pure `Reconciliation` domain type + resolve rule; both interpreters.

### 4. Alerting metric
- OTel counter `reconciliation.flags.recorded`, attribute `reason`,
  incremented only when a flag row is actually inserted, after the
  transaction commits; metric failure never fails the consumer.
- Convert the transition's reconciliation insert to `RETURNING` so the
  metric counts real inserts.

## Non-Functional Requirements
- TDD; `sbt scalafmtCheck test` green; coverage >80% on new code.
- No Kafka/registry changes; no cache changes.
- Update every anonymous `OrderStore` test stub with the new methods.
- Update `scripts/verify-rest-docs.sh`'s allowed-status map and the README.

## Acceptance Criteria
- Admin list returns all orders, filters by status/customer, newest first,
  bypasses the cache, and rejects an unknown status with 400.
- Queue lists open and resolved rows, filterable by status/orderId.
- Resolve requires the token, records operator/note/timestamp, is
  idempotent, and returns 404 for an un-flagged order.
- Concurrent resolves produce one winner; the loser sees the winner's row.
- A recorded flag increments the counter once; duplicates do not.
- Docs-coverage and full suites green.

## Out of Scope
- The other three services' reconciliation APIs (independent tracks).
- gshop aggregation/UI (ADM-7); live demo (ADM-9).
- Reopening resolved flags; pagination.
