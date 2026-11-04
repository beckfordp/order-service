# Plan: OTLP trace export to Tempo (gluon PERF-5.3)

## Phase 1: Export to Tempo
- [x] Task: purerestlib 0.8.2 → 0.10.0 in `build.sbt` 5b8e913
- [x] Task: `Telemetry.scala`: `Tracing.console` → `Tracing.resource` with
      `TracingConfig.fromEnv(sys.env)`, `Left` fails startup 5b8e913
- [x] Task: Full `sbt scalafmtCheck test` (258 tests, green)
- [x] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md):
      redeploy, spans in Tempo by `service.name`
      Redeployed; acceptance 53/53 + stock ledger OK, 0 restarts; Tempo lists
      all 8 `service.name`s. With Tempo scaled to 0: no restarts.

## Review
- No High/Medium in this repo's diff (env read inside `Sync.defer`, `Left`
  raised, no `case _`, scalafmt clean).
- Low (not fixed): with the obs stack down the batch exporter logs one
  `SEVERE: Failed to export` line per batch (noted in the values comment).
- Cross-repo (gluon PLAN, not this track): outgoing HTTP clients don't use
  purerest `ClientTracing`, so sync calls start new traces; and exemplars
  don't appear (otel4s IOLocal context isn't visible to the Java SDK's
  exemplar reservoir).
