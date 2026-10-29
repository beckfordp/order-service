# TD-15.1 Implementation Plan

## Phase 1 — Order-event split

- [x] Write failing tests: `PaymentEventConsumerSuite` asserts `OrderConfirmed` / `OrderPaymentFailed {reason}` / `OrderCancelled` outbox payloads and rejects `OrderStatusChanged`; `AsyncApiChannelsSuite` / `OrderAsyncApiDocsSuite` expect 3 publish channels; run and confirm failure. — bc1c62a
- [x] Implement `OrderEvents.scala` (3 events, schemas), `PaymentEventConsumer` outbox writes, `AsyncApiChannels` (3 `Publish(Cdc)`). — bc1c62a
- [x] Refactor: remove old event/status enum code; grep repo for remaining `OrderStatusChanged`. — bc1c62a (only script references remain, deferred to Phase 5)
- [ ] Task: Conductor - User Manual Verification 'Phase 1 — Order-event split' (Protocol in workflow.md)

## Phase 2 — Typed `OrderId` at the boundary

- [x] Write failing tests: `OrderIdSuite` (valid/invalid parse round-trip), route malformed-id → 400, store tests using `OrderId`; confirm failure. — 62e722f
- [x] Implement `domain/OrderId` (opaque UUID, `Either.catchOnly` parse, tapir codec/schema); routes `path[OrderId]`; store trait/interp take `OrderId`; consumers parse `orderId` once. — 62e722f
- [x] Refactor: delete all `Try(UUID.fromString).toOption` / bare `UUID.fromString`; update fixtures and suites. — 62e722f
- [ ] Task: Conductor - User Manual Verification 'Phase 2 — Typed OrderId' (Protocol in workflow.md)

## Phase 3 — Config: `Uri` base URLs and `asyncApiHtmlDir`

- [x] Write failing tests: `OrderServiceConfigSuite` parses valid/invalid `Uri` and `asyncApiHtmlDir`; `AppRoutesSuite` passes the configured docs dir; confirm failure. — b9c0d4e
- [x] Implement config fields, client constructors taking `Uri`, `AppRoutes` parameter, `Main` wiring, `application.conf` entries (incl. `${?ASYNCAPI_HTML_DIR}`). — b9c0d4e
- [x] Refactor: remove `sys.env` and `Uri.unsafeFromString`. — b9c0d4e
- [ ] Task: Conductor - User Manual Verification 'Phase 3 — Config' (Protocol in workflow.md)

## Phase 4 — Observability, ScalaCheck, narrower constraints

- [x] Write failing tests: consumer logging assertions (unknown eventType logged; raw payload absent; byte length/error class present); add `munit-scalacheck` and properties (OrderId round-trip, item/total arithmetic); confirm failure. — f7e7edc
- [x] Implement logging changes, property suites, and narrow `Cors` (`Applicative`), `HealthRoutes` (`Applicative`/`Monad`), route endpoints (`Monad`/`MonadThrow`), clients (`Concurrent`/`Clock`). — f7e7edc (Cors narrowed to `Monad`; `HealthRoutes.routes`/`AppRoutes` keep `Async` where http4s requires it)
- [x] Refactor: keep `Async` only where skunk/Kafka/fs2 require it. — f7e7edc
- [ ] Task: Conductor - User Manual Verification 'Phase 4 — Observability & purity' (Protocol in workflow.md)

## Phase 5 — Verification, registry, docs

- [x] Update `scripts/verify-kafka-asyncapi-docs.sh` and `verify-order-status.sh` for the new event names. — c735eee
- [x] Run full suite + coverage (`sbt coverage test coverageReport`), `sbt scalafmtAll scalafmtSbt`, `sbt scalafmtCheckAll scalafmtSbtCheck test`. — 199 tests green, 88.52% statement / 87.90% branch coverage
- [x] Run `sbt schemaRegistryCheck`; delete `order-events-OrderStatusChanged` from the local Apicurio registry; confirm 3 new artifacts registered. — registry group `gluon` holds only the 3 new `order-events-*` artifacts; no `OrderStatusChanged` artifact
- [x] Run the live `scripts/verify-kafka-asyncapi-docs.sh` (and the order-status script) against a running instance. — both scripts passed
- [x] Task: Conductor - User Manual Verification 'Phase 5 — Verification & registry' (Protocol in workflow.md) — covered by the two live scripts above

## Phase: Review Fixes

- [x] Task: Apply review suggestions 7f285d7
