# Spec: OTLP trace export to Tempo (gluon PERF-5.3)

## Overview
Spans go to the console (`Tracing.console`), so nothing reaches Tempo, which
gluon's obs stack now runs (ADR 0021). purerest 0.10.0 (PERF-5.2) adds
`Tracing.resource(serviceName, TracingConfig)`, exporting OTLP/HTTP when
`OTEL_EXPORTER_OTLP_ENDPOINT` is set.

## Functional Requirements
1. purerestlib 0.8.2 → 0.10.0.
2. `Telemetry.scala` builds the tracer with `Tracing.resource`, config from
   `TracingConfig.fromEnv(sys.env)`; a malformed `OTEL_TRACES_SAMPLER_ARG`
   fails startup (no silent default).
3. Unset endpoint keeps today's console export.
4. gluon `environments/local/order-service.values.yaml` sets
   `OTEL_EXPORTER_OTLP_ENDPOINT=http://tempo:4318` (gluon repo).

## Acceptance Criteria
- No `Tracing.console` in `src/main`.
- `sbt scalafmtCheck test` green.
- Deployed: the service's spans searchable in Grafana → Tempo by
  `service.name`; no restarts with the obs stack down.

## Out of Scope
- traceparent through Kafka (PERF-5.4/5.5), business spans (PERF-5.9),
  log correlation ids (PERF-5.6).
