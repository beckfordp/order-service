# Spec: Journey telemetry (gluon PERF-4.1–4.4, 4.10, 4.11)

## Overview
gluon PLAN.md Performance 4 for order-service. Adopts purerestlib/purekafka 0.8.0
(runtime, pool, event-age, milestones) and adds order-journey business metrics, so the
Christmas Rush dashboard can show checkout outcomes, status flow and time-in-state.

## Functional Requirements
1. Bump purerestlib + purekafka to 0.8.0. `Main` passes `runtime.metrics` to
   `Telemetry.resource` → JVM + cats-effect runtime metrics (PERF-4.1).
2. Wrap the skunk pool with `PoolMetrics.instrument(meter, "order")` (PERF-4.3).
3. Both consumers (payment-events, fulfilment-events) record `EventAge` per record (PERF-4.2).
4. `UpdatedOrderRef` gains `from: OrderStatus` and `enteredAt: Instant` (previous
   `updated_at`), returned by the guarded UPDATE via a self-join (Postgres 16, no
   `RETURNING OLD`).
5. `JourneyMetrics` store decorator (PERF-4.4): `orders.status.transitions{from,to}`
   counter, `orders.status.time_in_state{status}` histogram (s, 1s–1d), and
   `orders.placed` counter on `createAuthorized`; milestone log lines
   `order_placed` / `order_<to>` via `Milestones.log` (PERF-4.11).
6. `checkout.outcome{outcome}` counter on `POST /orders`: `authorized`, `declined`,
   `out_of_stock`, `invalid`, `unavailable`, `error` (PERF-4.4).
7. `POST /orders` runs inside a `checkout` span with `order.id` attribute (PERF-4.10);
   consumer spans are Perf 5.

## Non-Functional Requirements
- No order id on metric labels. Coverage > 80% on new code. scalafmt clean.

## Acceptance Criteria
- Unit tests for outcome classification, decorator metrics/logs, transition `from`/
  `enteredAt` (Testcontainers Postgres suite).
- `scripts/verify-journey-telemetry.sh`: after a real checkout in gluon-local, the
  metrics port shows `checkout_outcome_total`, `orders_status_transitions_total`,
  `jvm_memory_used`, `cats_effect_runtime`, `db_client_connection_count`,
  `purekafka_consumer_event_age`.

## Out of Scope
- Outbox gauge (PERF-4.9): Debezium outbox rows are never marked published, so the
  table can't say what's unpublished; the hop is measured from Debezium's own
  `MilliSecondsBehindSource` (Connect JMX, already scraped) on the dashboard instead.
- Consumer spans / OTLP (Perf 5)
