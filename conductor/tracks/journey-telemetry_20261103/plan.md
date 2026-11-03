# Plan: Journey telemetry (order-service)

## Phase 1: Adopt 0.8.0 cross-cutting telemetry
- [ ] Task: Bump purerestlib/purekafka 0.8.0; `Telemetry.resource` takes
      `ioRuntimeMetrics`; `Main` passes `runtime.metrics`
- [ ] Task: Write failing tests, then wrap pool with `PoolMetrics`; `EventAge` in both
      consumers
- [ ] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md)

## Phase 2: Business metrics, milestones, span
- [ ] Task: Write failing tests: `UpdatedOrderRef.from/enteredAt` (Postgres suite),
      `JourneyMetricsSuite`, `CheckoutOutcomeSuite`
- [ ] Task: Implement self-join UPDATE, `JourneyMetrics` decorator, checkout outcome
      counter + `checkout` span; wire in `Main`/`AppRoutes`
- [ ] Task: Full `sbt scalafmtCheck test` + coverage
- [ ] Task: Conductor - User Manual Verification 'Phase 2' (Protocol in workflow.md)

## Phase 3: Deploy + live verification
- [x] Task: Bump purerest 0.8.2 — JFR runtime telemetry off (gluon TD-28) c2538c1
- [ ] Task: Build/deploy to gluon-local; `scripts/verify-journey-telemetry.sh`
- [ ] Task: Conductor - User Manual Verification 'Phase 3' (Protocol in workflow.md)
