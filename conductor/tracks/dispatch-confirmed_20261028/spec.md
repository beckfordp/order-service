# Specification: US-17.4 — DispatchConfirmed → Dispatched

## Overview

order-service consumes `fulfilment-events: DispatchConfirmed` and moves the
order from `authorized` to `dispatched`. Order-side half of Journey 3's success
path; failure handling is US-18.3 (separate track).

## Functional Requirements

- **FR-1 — Status.** `OrderStatus` gains `Dispatched`; migration `V10` widens
  the status CHECK constraint.
- **FR-2 — Consumer.** New `FulfilmentEventConsumer` on `fulfilment-events`
  (group `order-service-fulfilment-events`): null-safe deserializers, envelope
  decode, handles only `DispatchConfirmed` (other event types skipped), payload
  decode failures logged and dropped, processing DLQ-guarded
  (`fulfilment-events-dlq`).
- **FR-3 — Guard.** Only an order currently `authorized` transitions to
  `dispatched`. Unknown orderId or any other status is logged and skipped
  (normal no-op, offset committed).
- **FR-4 — Supervision.** `KafkaConsumers` supervises the new consumer
  independently and merges it with `PaymentEventConsumer`.
- **FR-5 — AsyncAPI/registry.** Subscribe `DispatchConfirmed` on
  `fulfilment-events`; `schemaRegistryCheck` cross-checks the producer artifact.
- **FR-6 — No extra event.** No `OrderStatusChanged` on dispatch.
- **FR-7 — Live proof.** Authorized order → fulfilment dispatch → order status
  `dispatched`.

## Non-Functional Requirements

- No new dependencies; existing Kafka/store patterns reused; `sbt
  scalafmtCheck test` green.

## Acceptance Criteria

- Authorized order moves to dispatched exactly once; replays/no-ops leave
  status unchanged; malformed payloads don't kill the stream.
- AsyncAPI + registry updated; live check passes.

## Out of Scope

- US-18.3, payment void, notification emails, gshop status display.
