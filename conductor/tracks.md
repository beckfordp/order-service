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
- future: surface StockEventConsumer startup/fiber failure (e.g. Kafka unreachable) via /health/ready or a distinct error log, instead of the server silently running with no consumer (US-5.2 review finding)

---
