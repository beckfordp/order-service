# Order review fixes — Implementation Plan

## Phase 1 — Pure lifecycle and checkout
- [x] Write failing tests: OrderLifecycle transition/property suite; confirm failure. — 8b26f5d
- [x] Implement `domain/OrderLifecycle` (events, pure transition, allowedFrom table) and `TransitionRejected`; extend `Checkout`/`Pricing` validation once. — 8b26f5d
- [x] Task: Conductor - User Manual Verification 'Phase 1 — Lifecycle' (Protocol in workflow.md) — covered by OrderLifecycleSuite

## Phase 2 — Store transitions and query correctness
- [x] Write failing tests: store transition results (UnknownOrder vs IllegalTransition), batched list, one-session get, reconciliation flag. — 8b26f5d
- [x] Implement `transition` in both interpreters, drop `create`/`update`; conditional UPDATE + status re-read; batched items; `flagReconciliation`. — 8b26f5d
- [x] Update all suites/fixtures to seed via `createAuthorized` instead of `update`. — 8b26f5d
- [x] Task: Conductor - User Manual Verification 'Phase 2 — Store' (Protocol in workflow.md) — covered by store suites

## Phase 3 — Routes, cache, cleanups
- [x] Write failing tests: PATCH/PUT legal-only (409), cache failure fallback, topic constant. — 8b26f5d
- [x] Implement route transitions + 409 mapping, cache fallback/logging, cleanups. — 8b26f5d
- [x] Task: Conductor - User Manual Verification 'Phase 3 — Routes' (Protocol in workflow.md) — covered by OrderRoutesSuite

## Phase 4 — Verification
- [x] Full suite + coverage + scalafmt; live scripts (`verify-capture-outcome.sh`,
      `verify-dispatched.sh`, `verify-dispatch-failed.sh`, `verify-order-status.sh`).
      — 197 tests, 87.2%/87.1% coverage; all four live scripts passed, including
      PATCH `cancelled` -> 200 and illegal transition -> 409.
- [x] Task: Conductor - User Manual Verification 'Phase 4 — Verification' (Protocol in workflow.md) — covered by the four live scripts

## Phase: Review Fixes

- [x] Task: Apply review suggestions 45de25c
