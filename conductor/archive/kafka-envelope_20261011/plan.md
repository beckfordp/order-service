# Plan: Migrate to order-events/inventory-events/payment-events with envelope

## Phase 1: Envelope + three-topic publish/consume

- [x] Task: Write failing `OrderEventPublisherSuite`/
  `StockEventConsumerSuite`/`PaymentEventConsumerSuite` tests against the
  new topics/envelope shape, including a new "ignores a different
  eventType" case in each consumer suite (Red) (3be111b)
- [x] Task: Add `EventEnvelope.scala`; update `OrderEventPublisher` to
  publish both event types to `order-events` wrapped in the envelope;
  update `StockEventConsumer`/`PaymentEventConsumer` to subscribe to
  `inventory-events`/`payment-events` respectively, decode the envelope,
  branch on `eventType`, collapsing each consumer's previous two-stream
  `.merge` into one subscription (Green) (389465f)
- [x] Task: Run full `sbt test`, confirm green (389465f, 139/139 passing)
- [x] Task: Conductor - User Manual Verification 'Envelope + three-topic
  publish/consume' (Protocol in workflow.md) (c963a99 — updated and
  live-ran all 5 `scripts/verify-*.sh` scripts against real docker-compose
  Kafka + order-service; all pass)
