# Order review fixes — Implementation Plan

## Phase 1 — Pure lifecycle and checkout
- [ ] Write failing tests: OrderLifecycle transition/property suite; confirm failure.
- [ ] Implement `domain/OrderLifecycle` (events, pure transition, allowedFrom table) and `TransitionRejected`; extend `Checkout`/`Pricing` validation once.
- [ ] Task: Conductor - User Manual Verification 'Phase 1 — Lifecycle' (Protocol in workflow.md)

## Phase 2 — Store transitions and query correctness
- [ ] Write failing tests: store transition results (UnknownOrder vs IllegalTransition), batched list, one-session get, reconciliation flag.
- [ ] Implement `transition` in both interpreters, drop `create`/`update`; conditional UPDATE + status re-read; batched items; `flagReconciliation`.
- [ ] Update all suites/fixtures to seed via `createAuthorized` instead of `update`.
- [ ] Task: Conductor - User Manual Verification 'Phase 2 — Store' (Protocol in workflow.md)

## Phase 3 — Routes, cache, cleanups
- [ ] Write failing tests: PATCH/PUT legal-only (409), cache failure fallback, topic constant.
- [ ] Implement route transitions + 409 mapping, cache fallback/logging, cleanups.
- [ ] Task: Conductor - User Manual Verification 'Phase 3 — Routes' (Protocol in workflow.md)

## Phase 4 — Verification
- [ ] Full suite + coverage + scalafmt; live scripts (`verify-capture-outcome.sh`,
      `verify-dispatched.sh`, `verify-dispatch-failed.sh`, `verify-order-status.sh`).
- [ ] Task: Conductor - User Manual Verification 'Phase 4 — Verification' (Protocol in workflow.md)
