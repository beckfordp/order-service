# Spec — Migrate order-service to purekafka AsyncAPI docs (ADR 0010)

## Overview
order-service's two Kafka consumers (`StockEventConsumer`,
`PaymentEventConsumer`) have REST-side docs (Swagger UI at `/docs`, via
`purerest.docs.Docs`) but no equivalent for their Kafka contracts. This
track adopts `purekafka`'s `AsyncApiChannel`/`purekafka.docs.Docs`
registration API (built in `purerest`'s `purekafka-asyncapi_20261017`
track, with `kafka-harness` as its reference consumer) to register and
serve order-service's Kafka event contracts, mirroring that reference
implementation. **Depends on** a prerequisite `purerest` track fixing
`AsyncApiDoc.generate`'s same-topic handling — it currently keys channels
by topic in a `ListMap`, silently dropping all but the last-registered
channel per topic — needed because both of order-service's topics
multiplex two event types each via the envelope pattern
(`inventory-events`: `StockReserved`/`StockReservationFailed`;
`payment-events`: `PaymentSettled`/`PaymentFailed`).

## Functional Requirements
1. Bump `io.github.beckfordp:purekafka` to the version published after the
   prerequisite purerest fix lands.
2. Add tapir `Schema[A]` derivation (alongside each event case class's
   existing circe `Codec`) for `StockReservedEvent`,
   `StockReservationFailedEvent`, `PaymentSettledEvent`,
   `PaymentFailedEvent`.
3. Register 4 `AsyncApiChannel[A]` instances — both event types per topic
   — reflecting the real multiplexed contract once the prerequisite fix
   supports it.
4. Mount `purekafka.docs.Docs.routes[F]` in `Main.scala` at a raw-spec
   path that doesn't collide with the existing OpenAPI `/docs` (e.g.
   `/docs/asyncapi.json`, matching kafka-harness's path convention).
5. Add order-service's own
   `generate-kafka-asyncapi-docs.sh`/`verify-kafka-asyncapi-docs.sh`
   (adapted from purerest's, same version pins/patched-template
   workaround) and a second route serving the generated static HTML
   (e.g. `/docs/asyncapi`), sibling to the raw-spec route — same split as
   Swagger's `/docs` vs `/docs/docs.yaml`.
6. Update order-service's README with a "View Kafka AsyncAPI docs"
   section.
7. Producer side: no registrations — Rework 4's outbox pattern already
   removed all direct-publish call sites (already settled in PLAN.md).

## Acceptance Criteria
- All 4 event types appear in the generated AsyncAPI document, none
  silently dropped.
- Raw-spec route returns the document; static-HTML route serves the
  generated page with the same content.
- New + existing tests pass; `scalafmtCheck` clean.

## Out of Scope
- The purekafka library fix itself (separate purerest track,
  prerequisite).
- Rollout to payment-service/inventory-service/notification-service or
  `pure-service-generator` (separate PLAN.md backlog items).
