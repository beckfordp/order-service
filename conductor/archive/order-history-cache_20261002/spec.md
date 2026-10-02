# Overview
Implements US-8.1: a `GET /orders` list endpoint returning a customer's
order history, cached in Redis (the first Redis integration anywhere in
Gluon — cart-service's own Redis rework is still unimplemented). Cache-aside
with a short TTL only — no explicit invalidation on write, so a write can
take up to the TTL to show up in the cached list (accepted staleness for a
read-mostly history view).

# Functional Requirements
- New `OrderStore.listByCustomer(customerId: String): F[List[(Order, List[OrderItem])]]`,
  ordered newest-first by `createdAt`, in both in-memory and Postgres impls
- New `GET /orders?customerId=<id>` endpoint (required query param, no auth —
  bare customerId, consistent with the rest of the service) returning
  `List[OrderResponse]` (reuses the existing DTO from `GET /orders/{id}`)
- New `OrderHistoryCache[F[_]]` abstraction wrapping `redis4cats`:
  `get(customerId): F[Option[List[OrderResponse]]]` / `set(customerId, value): F[Unit]`
  (TTL baked into the write, via whichever redis4cats string-with-expiry
  method the published sources confirm — e.g. `setEx`); cache-aside in the
  route handler (hit → return; miss → query `listByCustomer`, cache, return)
- New `RedisConfig(uri: String, historyTtlSeconds: Int)` + `OrderServiceConfig`
  field, `redis4cats` dependency, `redis` block in `application.conf`,
  `docker-compose.yml` Redis service (mirrors the Postgres/Kafka additions
  from prior tracks)

# Non-Functional Requirements
- `redis4cats-effects` + `redis4cats-log4cats` (pinned to the latest
  Scala-3-published version, confirmed via Maven Central during
  implementation) + `testcontainers-scala-redis` (test-scope, same
  `testcontainersScalaVersion` already pinned — confirmed available at that
  exact version)
- No pagination — returns the full history (flagged as a future improvement
  if volume ever matters)
- No explicit cache invalidation on write (checkout, status update) —
  TTL-only freshness, by design

# Acceptance Criteria
- [ ] `OrderStore.listByCustomer` returns a customer's orders newest-first,
  empty list for an unknown customer — tested against both in-memory and
  Postgres
- [ ] `GET /orders?customerId=X` returns `200` with that customer's order
  history as `List[OrderResponse]`
- [ ] A cache miss queries the store and populates the cache; a subsequent
  call within the TTL is served from cache without hitting the store
- [ ] After the TTL expires, the next call re-queries the store (tested with
  a short TTL against a real Redis via Testcontainers)
- [ ] `sbt scalafmtCheck test` passes

# Out of Scope
- Explicit cache invalidation/update on order create or status change
  (deliberate TTL-only tradeoff)
- Pagination
- Auth/identity verification that the caller actually owns `customerId`
