# Spec — Refactor Main.scala to a flat Resource composition

## Overview
`Main.scala` wires 8+ dependencies (tracer, meter, logger, Postgres-backed
order store, a backgrounded Kafka-consumer fiber, an http4s client, a Redis
history cache, an inventory client) and assembles ~80 lines of HTTP routes,
all via manually nested `Resource#use` callbacks 6 levels deep. This
violates the composition pattern documented in `purerest`'s own
`docs/functional-design-patterns-scala-haskell-category-theory.md`
("Scaling tagless final without Cake: bundling dependencies into
`Resource`-built modules"): a flat `for`-comprehension over `Resource`,
with genuinely-coupled dependencies grouped into small modules, and route
assembly factored into its own function `Main` just calls once. This is a
structural refactor only — no behavior, route, or API change.

## Functional Requirements
1. **`Telemetry[F]`** (new): bundles `tracer: Tracer[F]`, `meter: Meter[F]`,
   `logger: StructuredLogger[F]` — the three dependencies already always
   acquired and threaded together. `Telemetry.resource[F[_]: Async:
   LocalContextProvider](serviceName: String, metricsPort: Int): Resource[F,
   Telemetry[F]]`, composing `Tracing.console` + `Metrics.oteljava` +
   `Resource.eval(Logging.create(tracer, serviceName))`.
2. **`InventoryClient` gains its own resilient-construction helper**: the
   shared http4s `Client[F]` is built once in `Main` (`EmberClientBuilder`,
   matching the design doc's own "one shared httpClient, fanned out"
   example), and `InventoryClient.resilient[F[_]: Async](httpClient:
   Client[F], config: InventoryClientConfig, telemetry: Telemetry[F]):
   InventoryClient[F]` wraps it with `Resilience.middleware` before
   delegating to the existing `apply` — a plain (non-`Resource`) function,
   directly unit-testable against a stub `Client[F]`. No `Clients[F]`
   wrapper, since there's only one client today (the doc's own multi-client
   bundling example is explicitly "illustrative only" for a fanout
   order-service doesn't have yet).
3. **`AppRoutes.all[F[_]: Async]`** (new): takes `store: OrderStore[F]`,
   `inventoryClient: InventoryClient[F]`, `historyCache: OrderHistoryCache[F]`,
   `telemetry: Telemetry[F]`, `channels: List[AsyncApiChannel[?]]` and
   returns the fully-assembled, fully-middleware-wrapped (CORS + server
   tracing + server metrics) `HttpRoutes[F]` ready to hand to
   `EmberServerBuilder`. Contains the existing docsRoutes/asyncApiRoutes/
   staticAsyncApiRoutes construction and combination (including the existing
   route-ordering comment/logic), moved verbatim out of `Main`.
4. **`Main.run` becomes a single flat `for`-comprehension over `Resource`**
   (terminated by `.useForever`), each dependency acquired via exactly one
   `<-` line: config → port → `Resource.eval(Migrations.run)` →
   `Telemetry.resource` → `OrderStore.postgres` → `KafkaConsumers.supervised
   (...).compile.drain.background` → shared `EmberClientBuilder` client →
   `InventoryClient.resilient` (plain value, not a `<-` bind) →
   `OrderHistoryCache.resource` → `routes = AppRoutes.all(...)` →
   `EmberServerBuilder...build`. No nested `.use` calls anywhere in `Main`.

## Non-Functional Requirements
- Zero behavioral change: identical routes, identical middleware order,
  identical startup log line, identical shutdown/resource-release order
  (the `for`-comprehension's acquisition order must match today's nesting
  order exactly, since `Resource` releases in reverse-acquisition order —
  same guarantee nested `.use` already gave).
- No new dependencies.
- Each new/changed module (`Telemetry`, `InventoryClient.resilient`,
  `AppRoutes.all`) keeps the same tagless-final shape (trait/case class +
  companion constructor) as every existing module in this codebase.

## Acceptance Criteria
- `Main.scala`'s `run`/equivalent is a single `for`-comprehension with no
  nested `Resource#use` calls.
- All existing tests pass unchanged (this is a pure refactor — existing
  suites already test `OrderRoutes`/`InventoryClient`/`OrderStore` etc.
  directly with constructed/mocked dependencies, not via `Main`).
- Manual smoke test: `sbt run` the service, confirm `/docs` (Swagger),
  `/docs/asyncapi.json`, `POST /orders`, and `GET /orders/{id}` all still
  work exactly as before.
- `sbt scalafmtCheckAll test` clean.

## Out of Scope
- Any `Clients[F]`-style bundling of `InventoryClient` with other clients —
  revisit only once order-service actually gains a second client.
- Any change to route behavior, middleware order, logging content, or the
  AsyncAPI/Swagger docs themselves.
- `KafkaConsumers.supervised(...).background`'s own internal implementation
  — only how `Main` acquires it changes (from a nested `.use` to a `<-`
  bind), not its logic.
