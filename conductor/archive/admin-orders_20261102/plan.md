# Plan: ADM-4 — admin order list + reconciliation workflow

## Phase 1: Reconciliation persistence and domain

- [x] Task: Failing `MigrationsSuite` assertions for the new columns, checks
  and index; add `V15__extend_order_reconciliation.sql` (Red → Green).
- [x] Task: Failing `ReconciliationDomainSuite` for status parsing and the
  pure resolve rule; add `domain/Reconciliation.scala` (Red → Green).
- [x] Task: Conductor - User Manual Verification 'Persistence and domain'
  (Protocol in workflow.md) — prompting off: committed suites.

## Phase 2: Store

- [x] Task: Failing in-memory + Postgres `OrderStore` tests for
  `listReconciliations` (filters), `resolveReconciliation` (winner, repeat,
  unknown, concurrent), and `listAll` (filters, cache bypass); implement both
  interpreters with the single-transaction conditional update.
- [x] Task: Failing `Metrics.test` assertion that a real flag insert
  increments `reconciliation.flags.recorded` once and a duplicate does not;
  convert the transition insert to `RETURNING` and add the counter (Green).
- [x] Task: Conductor - User Manual Verification 'Store'
  (Protocol in workflow.md) — prompting off: committed suites.

## Phase 3: Routes

- [x] Task: `OperatorAuth.requireOperator` + failing `OperatorAuthSuite`
  cases (absent/malformed/mismatched → 401); implement (Red → Green).
- [x] Task: Failing `ReconciliationRoutesSuite` (GET filters, PATCH 200/400/
  401/404, idempotency) and admin-list route tests; add
  `ReconciliationRoutes.scala` + the admin endpoint in `OrderRoutes`, register
  in `AppRoutes`/`routes` (Green).
- [x] Task: Update `OrderDocsSuite` expected statuses and every anonymous
  `OrderStore` stub; run `sbt scalafmtCheck test`.
- [x] Task: Conductor - User Manual Verification 'Routes'
  (Protocol in workflow.md) — prompting off: full suite green (243 passed).

## Phase 4: Wrap-up

- [x] Task: Update README/product docs and `scripts/verify-rest-docs.sh`,
  then run `sbt scalafmtCheck test`.
- [x] Task: Conductor - User Manual Verification 'Wrap-up'
  (Protocol in workflow.md) — prompting off: full suite green.

## Review

- Reviewed 2026-11-02 (conductor:review): plan compliance yes; style pass —
  pure resolve rule, conditional single-session update, insert-only counter
  after commit. Tests cover filters, idempotent/concurrent resolve, cache
  bypass, docs statuses and the metric. Full suite 243 passed. No blocking
  findings; first-wins terminality and the anonymous-email gap are documented
  (ADR 0026 / ADM-10).
