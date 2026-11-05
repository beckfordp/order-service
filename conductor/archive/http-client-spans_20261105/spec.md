# Spec: PERF-5.10 — HTTP client spans (ClientTracing) (gluon PERF-5.10)

## Overview
A checkout trace held only order-service spans: service-to-service HTTP calls
started new traces, and HTTP spans were kind INTERNAL. purerest 0.14.0's
`ClientTracing` opens a Client-kind span and injects its `traceparent`;
`ServerTracing` spans are Server-kind.

## Functional Requirements
1. purerestlib/purekafka 0.13.0 → 0.14.0.
2. Main: wrap the shared http4s client (inventory, payment calls) in `ClientTracing`, so each call is a Client-kind span and the callee joins the trace.

## Acceptance Criteria
- `sbt scalafmtCheck test` green.
- Live: a checkout/dispatch trace shows this service's client span with the callee's SERVER span as its child.

## Out of Scope
- Route-template span names; cart/catalog (no service-to-service callers).
