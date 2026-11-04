# Plan: OTLP trace export to Tempo (gluon PERF-5.3)

## Phase 1: Export to Tempo
- [x] Task: purerestlib 0.8.2 → 0.10.0 in `build.sbt` 5b8e913
- [x] Task: `Telemetry.scala`: `Tracing.console` → `Tracing.resource` with
      `TracingConfig.fromEnv(sys.env)`, `Left` fails startup 5b8e913
- [x] Task: Full `sbt scalafmtCheck test` (258 tests, green)
- [ ] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md):
      redeploy, spans in Tempo by `service.name`
