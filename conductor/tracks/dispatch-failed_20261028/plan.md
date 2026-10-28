# Plan: US-18.3 — DispatchFailed → dispatch_failed + release

## Phase 1: Status + migration

- [x] Task: Failing tests — `OrderStatus.DispatchFailed` parses/serializes; migration `V11` allows `dispatch_failed` (Red) `5dba50c`
- [x] Task: Implement the status and `V11__add_dispatch_failed_status.sql` (Green) `5dba50c`
- [x] Task: Refactor, `sbt scalafmtCheck test` green `5dba50c`
- [x] Task: Conductor - User Manual Verification 'Status + migration' (Protocol in workflow.md) `5dba50c`

## Phase 2: Consumer failure path + release

- [x] Task: Failing consumer tests (Kafka): synthetic `DispatchFailed` moves an authorized order to `dispatch_failed` and calls the stub release once; a non-authorized order is skipped with no release; duplicate event no-ops (Red) `5dba50c`
- [x] Task: Extend `FulfilmentEventConsumer` with `DispatchFailed` decoding + conditional status update + best-effort `InventoryClient.release`; wire the client in `KafkaConsumers`/`Main` (Green) `5dba50c`
- [x] Task: Refactor, full suite green `5dba50c`
- [x] Task: Conductor - User Manual Verification 'Consumer failure path + release' (Protocol in workflow.md) `5dba50c`

## Phase 3: AsyncAPI, registry, live proof, docs

- [x] Task: Register Subscribe `DispatchFailed` on `fulfilment-events`; run `schemaRegistryCheck` `5dba50c`
- [x] Task: Update `gluon/docs/system-design.md`, README/product/tech-stack `5dba50c`
- [x] Task: Rebuild/redeploy order-service; `scripts/verify-dispatch-failed.sh` — forced fulfilment failure → order `dispatch_failed` and reserved quantity restored; non-authorized order unchanged; cleanup `5dba50c`
- [x] Task: Conductor - User Manual Verification 'AsyncAPI, registry, live proof, docs' (Protocol in workflow.md) `5dba50c`

## Risks / notes

- Release is idempotent and best-effort after the status flips.
- No OrderStatusChanged on failure.
- Duplicate DispatchFailed events find the order already dispatch_failed and no-op.
