# Project Tracks

This file tracks all major tracks for the project.

- [x] **Track: Rework 7 order-service — sync-payment-auth Kafka contracts**
  *Link: [./tracks/payment-kafka-contracts_20261026/](./tracks/payment-kafka-contracts_20261026/)*

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- fix: reservation-failure response falls back to stale `pending` status if OrderStore.update races to None (US-4.2 review finding)

---
