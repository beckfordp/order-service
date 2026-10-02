# Project Tracks

This file tracks all major tracks for the project.

- [x] **Track: US-5.2: consume inventory.stock-reserved / inventory.stock-reservation-failed, update order status**
  *Link: [./tracks/stock-event-consumer_20261002/](./tracks/stock-event-consumer_20261002/)*

---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- US-8.1: order history read endpoint + Redis cache
- fix: reservation-failure response falls back to stale `pending` status if OrderStore.update races to None (US-4.2 review finding)

---
