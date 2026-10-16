# Project Tracks

This file tracks all major tracks for the project.

- [x] **Track: Transactional outbox for order-service (TD-3.2, ADR 0008)**
  *Link: [./tracks/outbox_20261012/](./tracks/outbox_20261012/)*

---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- fix: reservation-failure response falls back to stale `pending` status if OrderStore.update races to None (US-4.2 review finding)
- TD-2.2 / TD-2.4 (ADR 0010): `StockEventConsumer` adopts `purekafka`'s stream supervision (restart-with-backoff on any stream error, not just a null key/value — same `ConsumerSettings[F, String, String]` crash risk TD-2.1 already fixed for payment-service) and dead-letter routing (a message that still fails after retries goes to `<topic>-dlq` instead of crashing or blocking the partition). Supersedes the older, narrower idea of just *surfacing* the failure via `/health/ready` — `purekafka` actually recovers from it instead. Requires `io.github.beckfordp:purekafka` published (`sbt publishLocal` in the sibling `purerest` checkout) before this can compile against it.

---
