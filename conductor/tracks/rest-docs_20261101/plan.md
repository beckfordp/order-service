# Plan — order-service: REST docs convention (gluon TD-20.3)

## Phase 1: purerestlib 0.7.3
- [ ] Task: bump `purerestlibVersion` 0.4.0 → 0.7.3 (+ aligned tapir/purekafka pins); fix API drift until suite green
- [ ] Task: Conductor - User Manual Verification 'Phase 1: purerestlib 0.7.3' (Protocol in workflow.md)

## Phase 2: Per-endpoint error outputs
- [ ] Task: tests first — route-level tests that each endpoint's documented error statuses match FR2 subsets (red) → split `orderErrorOutput` into per-endpoint `oneOf`s over shared variants; existing error-response tests stay green
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Per-endpoint error outputs' (Protocol in workflow.md)

## Phase 3: Descriptions and examples
- [ ] Task: coverage test first in `OrderDocsSuite` (`Docs.undocumented == Nil`, red) → approved summaries/descriptions/tags, param descriptions/examples, DTO/Address/error annotations + examples in `OrderRoutes`/`HealthRoutes`/`Address`; green
- [ ] Task: Conductor - User Manual Verification 'Phase 3: Descriptions and examples' (Protocol in workflow.md)

## Phase 4: Smoke test
- [ ] Task: `scripts/verify-rest-docs.sh` — run service, fetch `/docs/docs.yaml`, assert summaries/descriptions/examples + narrowed errors; scalafmtCheck; CI green
- [ ] Task: Conductor - User Manual Verification 'Phase 4: Smoke test' (Protocol in workflow.md)
