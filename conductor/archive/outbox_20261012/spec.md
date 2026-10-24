# Spec — TD-3.2: Transactional outbox for order-service (ADR 0008)

## Overview
order-service currently writes its order/order-item state change (Postgres)
and then, as a *separate* step, calls `OrderEventPublisher` to produce
directly to Kafka. If the process dies between the two, or the publish
itself fails, the DB and the event stream silently diverge (TD-3). This
track closes that gap for order-service by making the Kafka-bound event
part of the *same* Postgres transaction as the state change: write an
`outbox_events` row instead of calling a producer. A separate, independent
gluon-root track (TD-3.1) will later point a real Debezium connector at
this table — this track does not depend on that, and is fully provable
against Testcontainers-Postgres alone.

## Functional Requirements
1. New Flyway migration (`V7__create_outbox_events.sql`): `outbox_events`
   table — `id` (uuid, pk), `aggregate_id` (text, not null — holds
   `orderId`), `payload` (jsonb, not null — the complete `EventEnvelope`
   JSON: `eventId`/`eventType`/`timestamp`/`source`/`correlationId`/
   `payload`), `trace_parent` (text, nullable), `correlation_id` (text,
   not null), `created_at` (timestamptz, not null, default `now()`). No
   `sent`/`processed` column — cleanup is time-based (TD-3.1's CronJob),
   not consumption-tracked.
2. All three of `OrderStore`'s existing state-transition methods that
   currently precede a separate publish call — `update` (called from
   `OrderRoutes`' checkout-failure path), `updateStatusByItemId` (called
   from `StockEventConsumer`), and `updateStatusIfCurrent` (called from
   `PaymentEventConsumer`) — are changed to write the matching outbox row
   **inside the same `session.transaction`** as their existing UPDATE,
   instead of returning so a caller can `.flatTap` a publish afterward.
3. `OrderEventPublisher` (the trait, its Kafka-producer `Resource`,
   `noOp`) is deleted outright, along with `OrderEventPublisherSuite.scala`
   — no dormant/unused code left behind.
4. `Main.scala` wiring updated: the `OrderEventPublisher.resource(...).use
   { ... }` block and its threading through to routes/consumers is
   removed.
5. Test coverage (Testcontainers-Postgres, same pattern this repo already
   uses) for each of the three call sites, asserting: the right
   `outbox_events` row is written (correct `aggregate_id`, `payload`
   decodes back to the expected envelope/event shape, `correlation_id`),
   and it's written *atomically* with the state change — a test proving a
   rolled-back state change leaves no outbox row either.

## Non-Functional Requirements
- No live Kafka, Kafka Connect, or Debezium needed for any test in this
  track — fully provable against Testcontainers-Postgres.
- No change to any external HTTP response shape or order-status state
  machine — this is purely a delivery-mechanism swap, invisible to
  callers.

## Acceptance Criteria
- `outbox_events` table exists per the shape above.
- All three current `OrderEventPublisher` call sites instead write an
  outbox row in the same transaction as their state change.
- `OrderEventPublisher` and its test suite are gone from the codebase.
- An atomicity test exists and passes (failed state change ⇒ no outbox
  row).
- Full existing test suite still passes — no behavior change visible
  outside this service.

## Out of Scope
- TD-3.1: Kafka Connect + Debezium connector deployment (gluon-root,
  `infra/k8s/local-infra/`) — separate, independent track.
- The Outbox Event Router SMT's own config (table→topic routing, header
  promotion) — lives with TD-3.1, not here.
- Outbox-row cleanup CronJob — also TD-3.1/gluon-root, per ADR 0008.
- inventory-service's and payment-service's own outbox migrations
  (TD-3.3/TD-3.4) — separate tracks in their own repos.
