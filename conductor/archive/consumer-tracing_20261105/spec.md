# Spec: consumers continue the trace (gluon PERF-5.5)

## Overview
Kafka messages carry the producer's `traceparent` header (PERF-5.4), but no
consumer reads it, so each async hop starts a new trace. purekafka 0.12.0's
`KafkaTracing.traced` joins it in a Consumer-kind span.

## Functional Requirements
1. purerestlib/purekafka → 0.12.0.
2. Every consumer handles each record inside
   `KafkaTracing.traced(tracer)(record.headers, s"$topic process", topic)`;
   the offset commit stays outside the span.
3. The tracer is threaded `Main` → `KafkaConsumers` → each consumer's `run`.
4. A missing or null `traceparent` header starts a new root span.

## Acceptance Criteria
- Test: a record with a `traceparent` header is handled in a `<topic> process`
  Consumer span whose parent is the header's span.
- `sbt scalafmtCheck test` green.
- Live: consumer spans appear under the producing request's trace in Tempo;
  outbox rows written while consuming carry `trace_parent`.

## Out of Scope
- HTTP client propagation (PERF-5.10), business spans (PERF-5.9).
