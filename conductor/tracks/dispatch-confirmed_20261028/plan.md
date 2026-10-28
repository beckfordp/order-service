# Plan: US-17.4 — DispatchConfirmed → Dispatched

## Phase 1: Status, migration, conditional store update

- [ ] Task: Failing tests — `OrderStatus.Dispatched` parses/serializes; migration `V10` allows `dispatched`; store gains `markDispatched(orderId)` that updates only from `authorized` (Red)
- [ ] Task: Implement the status, `V10__add_dispatched_status.sql`, and `markDispatched` in both store interpreters (Green)
- [ ] Task: Refactor, `sbt scalafmtCheck test` green
- [ ] Task: Conductor - User Manual Verification 'Status, migration, conditional store update' (Protocol in workflow.md)

## Phase 2: Consumer + supervision

- [ ] Task: Failing `FulfilmentEventConsumerSuite` (Kafka): synthetic `DispatchConfirmed` moves an authorized order to `dispatched`; non-authorized/unknown orders skipped; malformed payload logged; other eventTypes ignored (Red)
- [ ] Task: Implement `FulfilmentEventConsumer` and merge it independently supervised in `KafkaConsumers` (Green)
- [ ] Task: Refactor, full suite green
- [ ] Task: Conductor - User Manual Verification 'Consumer + supervision' (Protocol in workflow.md)

## Phase 3: AsyncAPI, registry, live proof

- [ ] Task: Register Subscribe `DispatchConfirmed` on `fulfilment-events`; run `schemaRegistryCheck`
- [ ] Task: Update `gluon/docs/system-design.md`, README/product/tech-stack
- [ ] Task: Rebuild/redeploy order-service; `scripts/verify-dispatched.sh` — authorized order → fulfilment simulate dispatch → order status `dispatched`; a forced-failure order stays `authorized` for now; cleanup
- [ ] Task: Conductor - User Manual Verification 'AsyncAPI, registry, live proof' (Protocol in workflow.md)

## Risks / notes

- Guard is a conditional `UPDATE ... WHERE status = 'authorized'`.
- No OrderStatusChanged on dispatch.
- DispatchFailed handling intentionally absent until US-18.3.
