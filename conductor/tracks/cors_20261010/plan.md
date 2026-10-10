# Plan: Backport CORS support from pure-service-generator (TD-1.2)

## Phase 1: CORS middleware

- [x] Task: Write failing `CorsSuite` (Red) — 3 tests against a dummy `HttpRoutes.of[IO]` wrapped in `Cors.middleware`, using order-service's `/orders` route: (1) GET w/ Origin → ACAO header; (2) OPTIONS preflight → correct Allow-Methods/Allow-Headers; (3) no Allow-Credentials. Confirm compile failure.
- [x] Task: Add `Cors.scala` (package `orderservice`) — verbatim port. Run `CorsSuite` — confirm green. `d2317a9`
- [x] Task: Wire `Cors.middleware` into `Main.scala`, wrapping the composed `routes` before `.withHttpApp(routes.orNotFound)`. Rerun full `sbt test` — confirm no regressions (137/137 passed). `d2317a9`
- [x] Task: Verify coverage (`sbt coverage test coverageReport`) — confirmed `Cors.scala` at 100% statement/branch coverage.
- [x] Task: Add `scripts/verify-cors.sh` (same shape as catalog/cart-service's, adapted: `GET /orders?customerId=...` instead of bare `GET /orders`, which requires a `customerId` query param) — real Postgres+Kafka+Redis + `sbt run`, curl GET+OPTIONS against `/orders`, assert headers, cleanup via `trap`. Ran for real — passed end-to-end. `0c8ae16`
- [x] Task: Conductor - User Manual Verification 'CORS middleware' — `/prompt off` active: ran `scripts/verify-cors.sh` directly — all checks passed (GET /orders?customerId=...: 200 + ACAO header, no Allow-Credentials; OPTIONS preflight: 200 + Allow-Methods includes POST + Allow-Headers includes Content-Type).

## Notes on `/prompt off`
Per-task Plan Mode review and commit-approval prompts skipped; each task committed directly, announced in one line.
