# Spec: traceparent into the outbox (gluon PERF-5.4)

## Overview
`outbox_events.trace_parent` exists and Debezium lifts it to a Kafka header,
but nothing writes it, so events carry no trace context. purerest/purekafka
0.11.0 adds `KafkaTracing.traceParent(tracer)`.

## Functional Requirements
1. purerestlib 0.10.0 → 0.11.0, purekafka → 0.11.0.
2. Every outbox insert writes `trace_parent` = the active span's W3C
   `traceparent`; NULL outside a span (e.g. Kafka consumers until PERF-5.5).
3. The store gets the service's tracer (`postgres(config, meter, tracer)`);
   skunk keeps its noop tracer (no DB spans, PERF-5.9).

## Acceptance Criteria
- Postgres suite: a write inside a span stores a traceparent whose trace id is
  the span's; outside a span stores NULL.
- `sbt scalafmtCheck test` green.
- Live: a row written by an HTTP request has `trace_parent`; the Kafka message
  carries it as a `traceparent` header (gluon Debezium alias).

## Out of Scope
- Consumers joining the trace (PERF-5.5); HTTP client propagation (PERF-5.10).
