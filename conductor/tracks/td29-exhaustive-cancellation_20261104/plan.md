# Plan: Exhaustive `OrderCancellation` match (gluon TD-29)

## Phase 1: Exhaustive match
- [ ] Task: Test: every `OrderEvent` has reason and actor both `Some` or both `None`
- [ ] Task: Replace `case _ => None` with the explicit non-cancelling cases in
      `reasonFor` and `actorFor`
- [ ] Task: Full `sbt scalafmtCheck test`
- [ ] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md)
