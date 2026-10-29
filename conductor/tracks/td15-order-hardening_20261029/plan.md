# TD-15.1 Implementation Plan

## Phase 1 — Order-event split

- [ ] Write failing tests: `PaymentEventConsumerSuite` asserts `OrderConfirmed` / `OrderPaymentFailed {reason}` / `OrderCancelled` outbox payloads and rejects `OrderStatusChanged`; `AsyncApiChannelsSuite` / `OrderAsyncApiDocsSuite` expect 3 publish channels; run and confirm failure.
- [ ] Implement `OrderEvents.scala` (3 events, schemas), `PaymentEventConsumer` outbox writes, `AsyncApiChannels` (3 `Publish(Cdc)`).
- [ ] Refactor: remove old event/status enum code; grep repo for remaining `OrderStatusChanged`.
- [ ] Task: Conductor - User Manual Verification 'Phase 1 — Order-event split' (Protocol in workflow.md)

## Phase 2 — Typed `OrderId` at the boundary

- [ ] Write failing tests: `OrderIdSuite` (valid/invalid parse round-trip), route malformed-id → 400, store tests using `OrderId`; confirm failure.
- [ ] Implement `domain/OrderId` (opaque UUID, `Either.catchOnly` parse, tapir codec/schema); routes `path[OrderId]`; store trait/interp take `OrderId`; consumers parse `orderId` once.
- [ ] Refactor: delete all `Try(UUID.fromString).toOption` / bare `UUID.fromString`; update fixtures and suites.
- [ ] Task: Conductor - User Manual Verification 'Phase 2 — Typed OrderId' (Protocol in workflow.md)

## Phase 3 — Config: `Uri` base URLs and `asyncApiHtmlDir`

- [ ] Write failing tests: `OrderServiceConfigSuite` parses valid/invalid `Uri` and `asyncApiHtmlDir`; `AppRoutesSuite` passes the configured docs dir; confirm failure.
- [ ] Implement config fields, client constructors taking `Uri`, `AppRoutes` parameter, `Main` wiring, `application.conf` entries (incl. `${?ASYNCAPI_HTML_DIR}`).
- [ ] Refactor: remove `sys.env` and `Uri.unsafeFromString`.
- [ ] Task: Conductor - User Manual Verification 'Phase 3 — Config' (Protocol in workflow.md)

## Phase 4 — Observability, ScalaCheck, narrower constraints

- [ ] Write failing tests: consumer logging assertions (unknown eventType logged; raw payload absent; byte length/error class present); add `munit-scalacheck` and properties (OrderId round-trip, item/total arithmetic); confirm failure.
- [ ] Implement logging changes, property suites, and narrow `Cors` (`Applicative`), `HealthRoutes` (`Applicative`/`Monad`), route endpoints (`Monad`/`MonadThrow`), clients (`Concurrent`/`Clock`).
- [ ] Refactor: keep `Async` only where skunk/Kafka/fs2 require it.
- [ ] Task: Conductor - User Manual Verification 'Phase 4 — Observability & purity' (Protocol in workflow.md)

## Phase 5 — Verification, registry, docs

- [ ] Update `scripts/verify-kafka-asyncapi-docs.sh` and `verify-order-status.sh` for the new event names.
- [ ] Run full suite + coverage (`sbt coverage test coverageReport`), `sbt scalafmtAll scalafmtSbt`, `sbt scalafmtCheckAll scalafmtSbtCheck test`.
- [ ] Run `sbt schemaRegistryCheck`; delete `order-events-OrderStatusChanged` from the local Apicurio registry; confirm 3 new artifacts registered.
- [ ] Run the live `scripts/verify-kafka-asyncapi-docs.sh` (and the order-status script) against a running instance.
- [ ] Task: Conductor - User Manual Verification 'Phase 5 — Verification & registry' (Protocol in workflow.md)
