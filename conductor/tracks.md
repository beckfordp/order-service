# Project Tracks

This file tracks all major tracks for the project.

- [x] **Track: Transactional outbox for order-service (TD-3.2, ADR 0008)**
  *Link: [./tracks/outbox_20261012/](./tracks/outbox_20261012/)*
- [x] **Track: order-service adopts purekafka (TD-2.2/TD-2.4, ADR 0010)**
  *Link: [./tracks/purekafka-adoption_20261016/](./tracks/purekafka-adoption_20261016/)*

---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- fix: reservation-failure response falls back to stale `pending` status if OrderStore.update races to None (US-4.2 review finding)

---
