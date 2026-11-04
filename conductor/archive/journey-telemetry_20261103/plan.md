# Plan: Journey telemetry (order-service)

## Phase 1: Adopt 0.8.0 cross-cutting telemetry
- [x] Task: Bump purerestlib/purekafka 0.8.0; `Telemetry.resource` takes
      `ioRuntimeMetrics`; `Main` passes `runtime.metrics` afe7e32
- [x] Task: Write failing tests, then wrap pool with `PoolMetrics`; `EventAge` in both
      consumers afe7e32
- [x] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md)

## Phase 2: Business metrics, milestones, span
- [x] Task: Write failing tests: `UpdatedOrderRef.from/enteredAt` (Postgres suite),
      `JourneyMetricsSuite`, `CheckoutOutcomeSuite` e0c5f92
- [x] Task: Implement self-join UPDATE, `JourneyMetrics` decorator, checkout outcome
      counter + `checkout` span; wire in `Main`/`AppRoutes` e0c5f92
- [x] Task: Full `sbt scalafmtCheck test` + coverage e3b1107
- [x] Task: Conductor - User Manual Verification 'Phase 2' (Protocol in workflow.md)

## Phase 3: Deploy + live verification
- [x] Task: Bump purerest 0.8.2 — JFR runtime telemetry off (gluon TD-28) c2538c1
- [x] Task: Build/deploy to gluon-local; `scripts/verify-journey-telemetry.sh` a7a86cb
- [x] Task: Conductor - User Manual Verification 'Phase 3' (Protocol in workflow.md)

## Phase: Review Fixes
- [x] Task: Apply review suggestions 28e1157
