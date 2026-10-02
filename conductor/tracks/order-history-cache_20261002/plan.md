# Implementation Plan: US-8.1 order history read endpoint + Redis cache

## Phase 1: OrderStore.listByCustomer (self-contained, no existing signatures change) [checkpoint: 05b1663]

- [x] Task: Add `OrderStore.listByCustomer(customerId: String): F[List[(Order, List[OrderItem])]]`, newest-first by `createdAt`, in both in-memory and Postgres impls; unit tests covering: a customer with multiple orders (ordering), a customer with none (empty list), orders belonging to a different customer excluded `05b1663`
- [x] Task: Conductor - User Manual Verification 'OrderStore.listByCustomer' (Protocol in workflow.md) `05b1663` - deviation: no new config/wiring/startup surface introduced this phase (pure store addition), so the real-Postgres Testcontainers tests already constitute the live verification; no separate shell script adds further confidence (unlike the Kafka-config phase, which risked a startup-only failure mode the test suite's self-constructed HOCON couldn't catch)

## Phase 2: Redis config + OrderHistoryCache (self-contained, no existing signatures change) [checkpoint: da289a1]

- [x] Task: Add `redis4cats-effects`/`redis4cats-log4cats`/`testcontainers-scala-redis` dependencies (versions confirmed against Maven Central at implementation time); add `redis` block to `application.conf` + `RedisConfig`/`OrderServiceConfig` field; add a Redis service to `docker-compose.yml` `64e6a29`
- [x] Task: Add `OrderHistoryCache[F[_]]` (`get`/`set` wrapping `redis4cats`, TTL from config) with an in-memory stub for route-level tests; Testcontainers-Redis tests: cache miss then hit, and expiry after the configured TTL re-misses `9564f2b`
- [x] Task: Conductor - User Manual Verification 'Redis config + OrderHistoryCache' (Protocol in workflow.md) `da289a1`

## Phase 3: Wire the GET /orders list endpoint (cache-aside)

- [ ] Task: Add `GET /orders?customerId=` tapir endpoint + server logic: cache hit -> return; miss -> `listByCustomer`, cache via `OrderHistoryCache.set`, return; wire into `OrderRoutes.routes` and `Main.scala` (construct the real Redis-backed cache)
- [ ] Task: `OrderRoutesSuite` tests using the in-memory cache stub: empty history, multiple orders newest-first, a cache hit skips the store (via a call-counting stub store)
- [ ] Task: Conductor - User Manual Verification 'GET /orders list endpoint' (final, Protocol in workflow.md)

Three phases, each independently testable before the next touches it -
matches the pattern from the Kafka consumer track (config/abstraction first,
then wire into routes last).
