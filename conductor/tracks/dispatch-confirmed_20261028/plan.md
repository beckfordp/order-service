# Plan: US-17.4 — DispatchConfirmed → Dispatched

## Phase 1: Status, migration, conditional store update

- [x] Task: Failing tests — `OrderStatus.Dispatched` parses/serializes; migration `V10` allows `dispatched`; store's existing `updateStatusIfCurrent` guard is exercised (Red) `e53809c` (no new store method needed)
- [x] Task: Implement the status, `V10__add_dispatched_status.sql`, and `markDispatched` in both store interpreters (Green) `e53809c`
- [x] Task: Refactor, `sbt scalafmtCheck test` green `e53809c`
- [x] Task: Conductor - User Manual Verification 'Status, migration, conditional store update' (Protocol in workflow.md) `e53809c`

## Phase 2: Consumer + supervision

- [x] Task: Failing `FulfilmentEventConsumerSuite` (Kafka): synthetic `DispatchConfirmed` moves an authorized order to `dispatched`; non-authorized/unknown orders skipped; malformed payload logged; other eventTypes ignored (Red) `e53809c`
- [x] Task: Implement `FulfilmentEventConsumer` and merge it independently supervised in `KafkaConsumers` (Green) `e53809c`
- [x] Task: Refactor, full suite green `e53809c`
- [x] Task: Conductor - User Manual Verification 'Consumer + supervision' (Protocol in workflow.md) `e53809c`

## Phase 3: AsyncAPI, registry, live proof

- [x] Task: Register Subscribe `DispatchConfirmed` on `fulfilment-events`; run `schemaRegistryCheck` `e53809c`
- [x] Task: Update `gluon/docs/system-design.md`, README/product/tech-stack `e53809c`
- [x] Task: Rebuild/redeploy order-service; `scripts/verify-dispatched.sh` — authorized order → fulfilment simulate dispatch → order status `dispatched`; a forced-failure order stays `authorized` for now; cleanup `e53809c`
- [x] Task: Conductor - User Manual Verification 'AsyncAPI, registry, live proof' (Protocol in workflow.md) `e53809c`

## Risks / notes

- Guard is a conditional `UPDATE ... WHERE status = 'authorized'`.
- No OrderStatusChanged on dispatch.
- DispatchFailed handling intentionally absent until US-18.3.
