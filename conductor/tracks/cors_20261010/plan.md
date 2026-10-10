# Plan: Backport CORS support from pure-service-generator (TD-1.2)

## Phase 1: CORS middleware

- [ ] Task: Write failing `CorsSuite` (Red) — 3 tests against a dummy `HttpRoutes.of[IO]` wrapped in `Cors.middleware`, using order-service's `/orders` route: (1) GET w/ Origin → ACAO header; (2) OPTIONS preflight → correct Allow-Methods/Allow-Headers; (3) no Allow-Credentials. Confirm compile failure.
- [ ] Task: Add `Cors.scala` (package `orderservice`) — verbatim port. Run `CorsSuite` — confirm green.
- [ ] Task: Wire `Cors.middleware` into `Main.scala`, wrapping the composed `routes` before `.withHttpApp(routes.orNotFound)`. Rerun full `sbt test` — confirm no regressions.
- [ ] Task: Verify coverage (`sbt coverage test coverageReport`) — confirm `Cors.scala` at 100%.
- [ ] Task: Add `scripts/verify-cors.sh` (same shape as catalog/cart-service's) — real Postgres+Kafka+Redis + `sbt run`, curl GET+OPTIONS against `/orders`, assert headers, cleanup via `trap`. Run for real — confirm pass.
- [ ] Task: Conductor - User Manual Verification 'CORS middleware' — `/prompt off` active: run `scripts/verify-cors.sh` directly, report result.

## Notes on `/prompt off`
Per-task Plan Mode review and commit-approval prompts skipped; each task committed directly, announced in one line.
