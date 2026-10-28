# Plan: US-18.3 — DispatchFailed → dispatch_failed + release

## Phase 1: Status + migration

- [ ] Task: Failing tests — `OrderStatus.DispatchFailed` parses/serializes; migration `V11` allows `dispatch_failed` (Red)
- [ ] Task: Implement the status and `V11__add_dispatch_failed_status.sql` (Green)
- [ ] Task: Refactor, `sbt scalafmtCheck test` green
- [ ] Task: Conductor - User Manual Verification 'Status + migration' (Protocol in workflow.md)

## Phase 2: Consumer failure path + release

- [ ] Task: Failing consumer tests (Kafka): synthetic `DispatchFailed` moves an authorized order to `dispatch_failed` and calls the stub release once; a non-authorized order is skipped with no release; duplicate event no-ops (Red)
- [ ] Task: Extend `FulfilmentEventConsumer` with `DispatchFailed` decoding + conditional status update + best-effort `InventoryClient.release`; wire the client in `KafkaConsumers`/`Main` (Green)
- [ ] Task: Refactor, full suite green
- [ ] Task: Conductor - User Manual Verification 'Consumer failure path + release' (Protocol in workflow.md)

## Phase 3: AsyncAPI, registry, live proof, docs

- [ ] Task: Register Subscribe `DispatchFailed` on `fulfilment-events`; run `schemaRegistryCheck`
- [ ] Task: Update `gluon/docs/system-design.md`, README/product/tech-stack
- [ ] Task: Rebuild/redeploy order-service; `scripts/verify-dispatch-failed.sh` — forced fulfilment failure → order `dispatch_failed` and reserved quantity restored; non-authorized order unchanged; cleanup
- [ ] Task: Conductor - User Manual Verification 'AsyncAPI, registry, live proof, docs' (Protocol in workflow.md)

## Risks / notes

- Release is idempotent and best-effort after the status flips.
- No OrderStatusChanged on failure.
- Duplicate DispatchFailed events find the order already dispatch_failed and no-op.
