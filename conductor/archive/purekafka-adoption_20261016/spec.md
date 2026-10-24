# Spec — TD-2.2/TD-2.4: order-service adopts purekafka (ADR 0010)

## Overview
order-service's two Kafka consumers (`StockEventConsumer`,
`PaymentEventConsumer`) are merged into one background fiber in
`Main.scala` with no restart-with-backoff and no dead-letter routing — a
transient broker blip or a genuinely malformed message currently either
crashes both consumers permanently (TD-2.2) or has no DLQ escape hatch
(TD-2.4). This track adopts `purekafka`'s `StreamSupervisor` and
`DeadLetter` combinators (built in the `purekafka_20261015` track,
`purerest` repo) to fix both, covering **both** consumers, not just
`StockEventConsumer` — agreed with the user since they're merged into the
same fiber today and leaving one unsupervised would be inconsistent.

## Functional Requirements
1. **Dependency**: add `io.github.beckfordp:purekafka`
   (`0.1.0+35-b414429d` — the exact locally-published version; no release
   tag exists yet) to `build.sbt`, same `resolvers`/pattern already used
   for `purerestlib`.
2. **Null-safety fix, folded in**: both consumers' `ConsumerSettings[F,
   String, String]` switch to `Option[String]` key/value (matching
   payment-service's existing TD-2.1 fix exactly). Necessary, not
   optional: a null-key/value crash happens at fs2-kafka's deserialization
   layer, *before* any processing function runs — `DeadLetter`'s
   bounded-retry-then-DLQ logic can never intercept it (it never receives
   the message), and without this fix `StreamSupervisor` would just
   restart into the same crash on the same still-unconsumed poison record
   forever.
3. **Stream supervision, per-consumer, before merging**: each of
   `StockEventConsumer.run`/`PaymentEventConsumer.run`'s own
   `Stream[F, Unit]` gets wrapped in `purekafka.consumer.StreamSupervisor`
   individually, *then* the two (now-supervised) streams are merged in
   `Main.scala` — not supervising the already-merged stream as one unit,
   so a crash in one consumer's stream doesn't force an unrelated restart
   of the other.
4. **Dead-letter routing, per consumer**: each consumer's per-message
   processing (the `handleReserved`/`handleReservationFailed`/
   `handleSettled`/`handleFailed` branches) gets wrapped in
   `purekafka.consumer.DeadLetter`, so a message that decodes fine but
   fails processing after retries lands on
   `inventory-events-dlq`/`payment-events-dlq` respectively, instead of
   being silently dropped (today's `decode` failure path) or crashing
   the stream.

## Non-Functional Requirements
- No change to either consumer's actual business logic (status updates,
  outbox writes from the TD-3.2 track) — this is purely a reliability
  wrapper around existing behavior.
- No change to `Main.scala`'s external behavior (`/health/ready`, HTTP
  routes) — scoped to the background Kafka fiber only.

## Acceptance Criteria
- A null key/value record no longer crashes either consumer (proven
  against a synthetic record, same pattern TD-2.1's own fix was verified
  with).
- A synthetic stream error restarts that consumer's stream with backoff,
  without affecting the other consumer's stream.
- A message that fails processing after retries lands on the correct
  `-dlq` topic; the stream keeps consuming subsequent messages.
- Full existing test suite still passes — no behavior change visible to
  callers.

## Out of Scope
- `payment-service`'s and `notification-service`'s own purekafka
  adoption — separate tracks in their own repos (same backlog pattern).
- AsyncAPI documentation, `purekafka`'s own further development —
  unrelated to this adoption work.
- Cutting an actual version tag/release for `purekafka` — this track
  pins the local `publishLocal` version as-is; a real release is a
  separate, future concern once more services have adopted it.
