# Spec: milestone logs without correlation_id (gluon PERF-5.6)

## Overview
Logs correlate by `trace_id`/`span_id` (purerest `Logging.traceCorrelated`)
and `order_id`. purerest 0.13.0's `Milestones.log` drops the always-`None`
`correlationId` argument.

## Functional Requirements
1. purerestlib/purekafka → 0.13.0.
2. `Milestones.log` calls drop the `None` correlation argument.

## Acceptance Criteria
- Journey-metrics milestone tests unchanged and green; `sbt scalafmtCheck test` green.
- Live: milestone lines carry `trace_id`/`span_id` of the business span they ran in.

## Out of Scope
- Kafka envelope `correlationId` / `correlation_id` header (payload contract).
