# Overview
Wire a resilient, synchronous HTTP call to inventory-service's
`POST /inventorys/reservations` into checkout (`POST /orders`), so stock is
reserved for every line item before the order succeeds. Uses purerest's
`Resilience.middleware` (retry + circuit breaker, same pattern as the
generator's `ClientResilienceExampleSuite`) wrapping a real http4s client
this time, not a stub. All-or-nothing reservation: if any item can't be
reserved, the order is still created (per US-3.1's atomic persistence) but
marked `reservation_failed`; if every item reserves, the order stays
`pending` (`pending` → `reserved` stays US-5.2's job).

# Functional Requirements
- New `InventoryClient[F[_]]` with `reserve(sku, quantity): F[ReservationResult]`,
  a closed ADT: `Reserved`, `InsufficientStock`, `UnknownSku` (200/409/404 per
  `gluon/docs/system-design.md`'s documented contract). An unexpected status
  or a resilience failure (circuit open, retries exhausted, connection
  error) raises an exception instead of returning a `ReservationResult` case.
- `InventoryClient`'s `Client[F]` wrapped with `Resilience.middleware`,
  configured via a new `inventory-client` block in `application.conf` (base
  URL + retry/circuit-breaker, same shape as `example-client`) — base URL
  overridable via env var.
- New `http4s-ember-client` dependency (none exists yet).
- Checkout calls `InventoryClient.reserve` once per item, sequentially,
  short-circuiting at the first non-`Reserved` result.
- Order + all items always persisted (existing atomic `OrderStore.create`,
  unchanged) regardless of reservation outcome.
  - All items reserve: status stays `pending`.
  - Any item fails (or the call itself fails): `OrderStore.update` sets
    status to `reservation_failed` before responding.
- Response is always `201 Created` either way — the order resource was
  created; `OrderResponse.status` communicates the actual outcome.
- Tested entirely against a stub `Client[F]` (`ClientResilienceExampleSuite`'s
  pattern) — no live inventory-service in this repo's tests.

# Non-Functional Requirements
- Resilience config externally configurable, not hardcoded
- Existing ADT/pure-FP conventions
- No change to `POST /orders`'s request/response JSON shape from US-3.1

# Acceptance Criteria
- [ ] `InventoryClient.reserve` returns `Reserved`/`InsufficientStock`/`UnknownSku`
  for 200/409/404, against a stub client
- [ ] A transient downstream failure is retried per config
- [ ] Repeated failures open the circuit breaker and fail fast
- [ ] `POST /orders`, all items reservable: order created, status `pending`,
  `201`
- [ ] `POST /orders`, one item fails reservation: order created with all
  items persisted, status `reservation_failed`, `201`
- [ ] `POST /orders`, inventory client raises (circuit open/retries
  exhausted): order still created with status `reservation_failed`, `201`
- [ ] `sbt scalafmtCheck test` passes

# Out of Scope
- **Partial order fulfillment** (flagged as a future improvement, not a
  final design decision)
- Any compensating release/undo call for already-reserved items
  (inventory-service has no such endpoint)
- US-5.2's async Kafka consumption (`pending` → `reserved` transition)
- A live integration test against a real inventory-service (PLAN.md: later,
  optional)
