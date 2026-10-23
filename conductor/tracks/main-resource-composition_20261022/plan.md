# Plan — Refactor Main.scala to a flat Resource composition

## Phase 1 — `Telemetry[F]` module [checkpoint: 4f0957c]
- [x] Task: Write failing test(s) in a new `TelemetrySuite`: `Telemetry[F]` bundles `tracer`/`meter`/`logger`; `Telemetry.resource[F[_]: Async: LocalContextProvider](serviceName, metricsPort): Resource[F, Telemetry[F]]` has the documented signature (Red) `eb88ebc`
- [x] Task: Implement `Telemetry.scala` (`Tracing.console` + `Metrics.oteljava` + `Resource.eval(Logging.create(tracer, serviceName))`) (Green) `eb88ebc` — deviation: split composition into a testable `fromResources(tracerResource, meterResource, serviceName)` plus a thin `resource(serviceName, metricsPort)` production wrapper, so the real wiring logic is unit-testable via `Tracing.test`/`Metrics.test` without binding a real Prometheus port (matches this codebase's own convention that `.console`/`.oteljava` are only ever exercised from `Main`); see git note on `eb88ebc`
- [x] Task: Conductor - User Manual Verification 'Phase 1: Telemetry[F] module' (Protocol in workflow.md) `4f0957c` — reduced-prompting mode: automated-test verification only, no interactive walkthrough; see git note on `4f0957c`

## Phase 2 — `InventoryClient.resilient` constructor [checkpoint: 6667d73]
- [x] Task: Write failing test(s): `InventoryClient.resilient[F[_]: Async](httpClient: Client[F], config: InventoryClientConfig, telemetry: Telemetry[F]): InventoryClient[F]` wraps the given client with `Resilience.middleware` before delegating to the existing `apply`, verified against a stub `Client[F]` (Red) `65e899e`
- [x] Task: Implement `InventoryClient.resilient` (Green) `65e899e`
- [x] Task: Conductor - User Manual Verification 'Phase 2: InventoryClient.resilient constructor' (Protocol in workflow.md) `6667d73` — reduced-prompting mode: automated-test verification only, no interactive walkthrough; see git note on `6667d73`

## Phase 3 — `AppRoutes.all` route-assembly extraction [checkpoint: 086190b]
- [x] Task: Write failing test(s) in a new `AppRoutesSuite`: `AppRoutes.all[F[_]: Async](store, inventoryClient, historyCache, telemetry, channels): HttpRoutes[F]` returns the fully-combined, fully-middleware-wrapped routes — `/docs` (Swagger) resolves, `/docs/asyncapi.json` resolves and contains the registered channel names, a business route (e.g. `POST /orders`) resolves against stub store/inventoryClient (Red) `44ccdfe`
- [x] Task: Implement `AppRoutes.scala`, moving the existing docsRoutes/asyncApiRoutes/staticAsyncApiRoutes construction, combination (with its existing route-ordering comment), and CORS/tracing/metrics middleware application verbatim out of `Main` (Green) `44ccdfe` — deviation: generalizing from concrete `IO` to abstract `F[_]: Async` surfaced a new fs2 `Files.implicitForAsync` deprecation warning on `fileService[F]`; fixed with an explicit `Files.forAsync[F]` given; see git note on `44ccdfe`
- [x] Task: Conductor - User Manual Verification 'Phase 3: AppRoutes.all extraction' (Protocol in workflow.md) `086190b` — reduced-prompting mode: automated-test verification only, no interactive walkthrough; see git note on `086190b`

## Phase 4 — Flatten `Main.run`
- [ ] Task: Rewrite `Main.run` as a single flat `for`-comprehension over `Resource` (config → port → `Resource.eval(Migrations.run)` → `Telemetry.resource` → `OrderStore.postgres` → `KafkaConsumers.supervised(...).background` → shared `EmberClientBuilder` client → `InventoryClient.resilient` → `OrderHistoryCache.resource` → `routes = AppRoutes.all(...)` → `EmberServerBuilder...build`), terminated by `.useForever`, with no nested `Resource#use` calls remaining
- [ ] Task: Run the full existing `order-service` test suite unchanged — confirm all pass, proving the refactor altered no behavior
- [ ] Task: Manual smoke test — `sbt run`, confirm `/docs`, `/docs/asyncapi.json`, `POST /orders`, `GET /orders/{id}` all work exactly as before
- [ ] Task: Conductor - User Manual Verification 'Phase 4: Flatten Main.run' (Protocol in workflow.md)
