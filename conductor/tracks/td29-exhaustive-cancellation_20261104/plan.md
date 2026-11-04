# Plan: Exhaustive `OrderCancellation` match (gluon TD-29)

## Phase 1: Exhaustive match
- [x] Task: Test: every `OrderEvent` has reason and actor both `Some` or both `None` 443baa1 (passes before the fix too: the guarantee is compile-time)
- [x] Task: Replace `case _ => None` with the explicit non-cancelling cases in
      `reasonFor` and `actorFor` 443baa1
- [x] Task: Full `sbt scalafmtCheck test` (258 tests, green)
- [x] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md)
