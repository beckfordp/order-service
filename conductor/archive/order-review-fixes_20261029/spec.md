# Order-service full review fixes (15 findings)

## Overview

Applies the remaining findings from the 2026-10-29 full-codebase review after
the cross-repo TD-15.1 work (typed `OrderId`, event split, config, logging,
ScalaCheck, narrowed bounds).

## Functional Requirements

1. **Pure lifecycle** — `domain/OrderLifecycle`: sealed `OrderEvent`, a pure
   `transition(current, event)` and the same rule table driving SQL guards.
   Every status change (HTTP, Kafka) goes through it.
2. **Typed transition result** — store returns
   `Either[TransitionRejected, UpdatedOrderRef]` (`UnknownOrder` vs
   `IllegalTransition`); 0-row conditional update re-reads status in the same
   transaction to distinguish 404 from 409.
3. **PATCH/PUT** — only legal lifecycle transitions (currently
   `authorized -> cancelled`); other valid statuses → 409, unknown → 400;
   successful PATCH/PUT writes the `OrderCancelled` outbox row.
4. **Store correctness** — drop dead `create`; one-session `get`; batched
   item fetch (no N+1); `flagReconciliation` when inventory release fails on
   `DispatchFailed`; distinct timer label for reconciliation reads.
5. **Routes/cache** — Redis errors fall back to Postgres with a warning;
   cache decode failures logged, not silently swallowed.
6. **Cleanups** — literal `fulfilment-events` topic uses the consumer
   constant; stale doc comments; dedupe `committerCommit`.
7. **Tests** — lifecycle properties, checkout/overflow coverage, updated
   store/route/consumer suites.

## Acceptance Criteria

- No arbitrary status write path remains.
- Full suite + scalafmt + live E2E scripts green.
