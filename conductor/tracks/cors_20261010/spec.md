# Spec: Backport CORS support from pure-service-generator (TD-1.2)

## Overview
order-service has no CORS support (confirmed 2026-10-09 via gshop's Checkout button). Same proven pattern as the generator's and catalog-service's/cart-service's own `cors_20261010` tracks — a direct, no-new-design port. This is the third and final service closing out TD-1.2.

## Functional Requirements
1. Add `Cors.scala` (package `orderservice`) — identical to the proven `Cors.middleware[F[_]: Async]`: allow-all origins, GET/POST/PATCH/PUT/DELETE, `Content-Type` header, credentials disallowed.
2. Wire `Cors.middleware` into `Main.scala`, wrapping the fully-composed routes before `.withHttpApp(routes.orNotFound)`.
3. Add `CorsSuite.scala` — same 3 assertions, against order-service's own `/orders` route (bare GET/POST both exist here).

## Non-Functional Requirements
- No new dependency.
- No change to order-service's domain routes/logic.

## Acceptance Criteria
- [ ] `CorsSuite` passes (3 tests), 100% coverage on `Cors.scala`.
- [ ] Full `sbt test` suite green, no regressions.
- [ ] Real verification via `scripts/verify-cors.sh`: GET `/orders` with `Origin` shows ACAO (not POST, since POST /orders calls out to inventory-service synchronously and would need that service running too — out of scope for this check); OPTIONS preflight shows correct Allow-Methods/-Headers.

## Out of Scope
- The CORS policy itself (already decided at the generator level).
- inventory-service, payment-service, notification-service (TD-1.3) — separate tracks.
