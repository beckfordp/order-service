# Implementation Plan: US-4.2 wire resilience middleware for the reserve call to inventory-service

## Phase 1: InventoryClient + resilience wiring (self-contained, no existing signatures change) [checkpoint: 69c3c3b]

- [x] Task: Add http4s-ember-client dependency; add inventory-client application.conf block (base URL + retry/circuit-breaker) and a matching OrderServiceConfig field `0affebd`
- [x] Task: Add ReservationResult ADT (Reserved/InsufficientStock/UnknownSku) and InventoryClient trait + implementation wrapping Client[F] with Resilience.middleware; unit tests against a stub Client[F] covering status-code mapping, retry-until-success, and circuit-breaker-opens-and-fails-fast (reusing ClientResilienceExampleSuite's test patterns) `a067557`
- [x] Task: Conductor - User Manual Verification 'InventoryClient + resilience wiring' `69c3c3b`

## Phase 2: Wire into checkout

- [x] Task: Update OrderRoutes' create logic to accept an InventoryClient[F], call reserve per item sequentially with short-circuit on the first failure, and call OrderStore.update to set reservation_failed when any item fails or the client raises; update Main.scala to construct and wire the real InventoryClient `1e33d1e`
- [x] Task: Extend OrderRoutesSuite with a stub InventoryClient for the three checkout scenarios (all succeed -> pending/201, one fails -> reservation_failed/201 with all items still persisted, client raises -> reservation_failed/201) `1e33d1e`
- [ ] Task: Conductor - User Manual Verification 'US-4.2 wire resilience middleware for the reserve call to inventory-service' (final, Protocol in workflow.md)

Two phases since InventoryClient is purely additive (no existing signature
changes) and can be fully built/tested/committed on its own before
checkout's create logic is touched - unlike the last two tracks, this split
should actually hold.
