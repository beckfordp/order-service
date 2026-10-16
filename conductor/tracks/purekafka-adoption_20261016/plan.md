# Plan — TD-2.2/TD-2.4: order-service adopts purekafka (ADR 0010)

## Phase 1 — Dependency + null-safety [checkpoint: ff15c46]
- [x] Task: Add `io.github.beckfordp:purekafka` dependency to `build.sbt` (same resolver pattern as `purerestlib`); confirm `sbt compile` succeeds `ee08e70`
- [x] Task: Write failing tests: a null key and a null value record don't crash `StockEventConsumer`/`PaymentEventConsumer` (Red) `163ad2f`
- [x] Task: Switch both consumers' `ConsumerSettings[F, String, String]` to `Option[String]` key/value, matching payment-service's TD-2.1 fix (Green) `163ad2f`
- [x] Task: Conductor - User Manual Verification 'Phase 1: Dependency + null-safety' (Protocol in workflow.md) `ff15c46` — reduced-prompting mode: automated-test verification only (145 passed), no interactive walkthrough; see git note on `ff15c46`

## Phase 2 — Per-consumer stream supervision
- [ ] Task: Write failing test: a synthetic stream error in `StockEventConsumer`'s own stream triggers a `purekafka.consumer.StreamSupervisor` restart, without killing or restarting `PaymentEventConsumer`'s separately-supervised stream (Red)
- [ ] Task: Wrap each of `StockEventConsumer.run`/`PaymentEventConsumer.run`'s `Stream[F, Unit]` individually in `StreamSupervisor` inside `Main.scala`, *then* merge the two supervised streams (Green)
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Per-consumer stream supervision' (Protocol in workflow.md)

## Phase 3 — Dead-letter routing
- [ ] Task: Write failing tests: a message that fails processing after retries lands on `inventory-events-dlq`/`payment-events-dlq` respectively, and the stream keeps consuming subsequent messages (Red)
- [ ] Task: Wrap each consumer's per-message processing branch in `purekafka.consumer.DeadLetter` (Green)
- [ ] Task: Run the full existing test suite, confirm no regressions in business logic (status updates, TD-3.2's outbox writes)
- [ ] Task: Conductor - User Manual Verification 'Phase 3: Dead-letter routing' (Protocol in workflow.md)
