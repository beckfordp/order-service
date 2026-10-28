# Tech Stack — order-service

## Language / runtime
- Scala 3.9.0
- JDK 21

## Effects / HTTP
- Cats Effect 3.7.0
- http4s 0.23.37 (ember-server, ember-client — client used by `InventoryClient`
  for the resilience-wrapped sync reserve call to inventory-service)
- circe 0.14.16
- CORS — `org.http4s.server.middleware.CORS` (ships in `http4s-server`, no new
  dependency) wraps every response (`Cors.scala`). Allow all origins, methods
  limited to this service's five CRUD routes, `Content-Type` header allowed,
  credentials disallowed. Ported from `pure-service-generator`'s
  `cors_20261010` track (TD-1.2).
- fs2-kafka 3.6.0 — consumer side: `PaymentEventConsumer` subscribes to
  `payment-events` (`PaymentCaptured`/`PaymentAuthorizationExpired`/
  `PaymentFailed`; Rework 7) and `FulfilmentEventConsumer` subscribes to
  `fulfilment-events` (`DispatchConfirmed`/`DispatchFailed`, US-17.4/18.3). The earlier
  `StockEventConsumer` (`inventory-events`) was retired under ADR 0018
  (TD-7.3).
  Producer side (`OrderEventPublisher`,
  added for US-5.3) was deleted in TD-3.2/ADR 0008: publishing an
  `order-events` event is now a write to the
  `outbox_events` table inside the same Postgres transaction as the state
  change it announces (see `OrderStore.PendingOutboxEvent`), not a direct
  Kafka produce call
- purekafka (`io.github.beckfordp`) — `consumer.StreamSupervisor` wraps
  each consumer's stream (restart-with-backoff, merged) and
  `consumer.DeadLetter` wraps its per-message processing (bounded retry, then
  routes to `payment-events-dlq` on exhaustion) — TD-2.2/TD-2.4, ADR 0010.
  Wired together in `KafkaConsumers.supervised`
- cats-retry (`com.github.cb372`) — not an explicit `build.sbt` dependency;
  comes transitively via `purerestlib`, which uses it internally for
  `purerest.resilience`'s own `Client[F]` retry middleware. Used directly
  (not through `purerest.resilience`, which is `Client[F]`-only and doesn't
  cover a Kafka producer call) to bound-retry `OrderEventPublisher`'s
  publish, matching the pattern payment-service's `PaymentEventPublisher`
  established the same day for the identical problem (deviation found
  2026-10-02: `system-design.md`'s "Reliability" note originally said
  publishes would be `purerest.resilience`-wrapped; corrected here and
  there once that turned out not to apply to Kafka)

## API layer
- tapir 1.11.25 — route definitions and generated Swagger/OpenAPI docs
  (`purerest.docs.Docs`), served at `/docs`, kept in sync by construction
- purekafka.docs (`AsyncApiChannel`/`Docs`) — generates AsyncAPI documentation
  for this service's Kafka event contracts (`AsyncApiChannels`), served at
  `/docs/asyncapi.json` (raw spec) and `/docs/asyncapi` (rendered static HTML,
  via `scripts/generate-kafka-asyncapi-docs.sh`) — same tapir-OpenAPI pattern,
  for the Kafka side
- `schemaRegistryCheck` (sbt-purekafka-docs, Rework 5 step 3/ADR 0003) —
  reuses the same `AsyncApiChannels` registrations to check/register each
  event's JSON Schema against Apicurio Registry, run explicitly (not
  chained after `compile`): locally against the shared OrbStack instance
  (`schemaRegistryUrl`'s default), in CI against a throwaway
  `docker run` Apicurio container (`SCHEMA_REGISTRY_URL` env override —
  see `ci.yml`). Build/CI-time only, zero runtime presence.

## Persistence
- skunk 1.0.0 — pure-FP, non-blocking Postgres access (runtime queries)
- Flyway 11.8.2 (+ flyway-database-postgresql) — schema migrations on startup
- postgresql JDBC 42.7.13 — Flyway-only (runtime scope), never used directly
  in application code

## Caching
- redis4cats-effects + redis4cats-log4cats 2.0.6 — cats-effect-native Redis
  client (US-8.1), caches a customer's order history (`OrderHistoryCache`),
  cache-aside with TTL-only freshness. First Redis integration anywhere in
  Gluon — no prior pattern existed to follow

## Config
- pureconfig 0.17.10 — typed config from `application.conf`

## Shared platform library
- `purerestlib` 0.1.0 (`io.github.beckfordp`) — tracing, structured logging,
  metrics, resilience (retry + circuit breaker), tapir-based docs. Consumed
  as a published GitHub Packages artifact, never vendored/`.dependsOn`.
  ADR 0005 flags a possible future migration off GitHub Packages (JitPack or
  Maven Central) — not yet adopted.

## Testing
- munit 1.3.6 + munit-cats-effect 2.2.1
- log4cats-testing 2.8.0 — assert on structured log output
- testcontainers-scala 0.43.6 (postgresql + kafka + redis + munit modules) —
  real, ephemeral Postgres/Kafka/Redis for integration tests, no manual
  local setup
- scalafmt (default Scala 3 style) — `sbt scalafmtCheck test` run in CI

## Packaging / local deploy
- sbt-native-packager (`JavaAppPackaging`, `DockerPlugin`)
- Docker image: `eclipse-temurin:21-jre`
- Docker Compose — local Postgres + Kafka + Redis

## Target infrastructure (platform-wide, from `gluon/docs/system-design.md`)
- Local: OrbStack Kubernetes (see ADR 0002)
- Promotion: dev → staging → prod, all on AWS EKS, single AWS account /
  multi-namespace (see ADR 0004)
- CI/CD: GitHub Actions, candidate release = container image tag promoted
  through environments with automated smoke-test gates
