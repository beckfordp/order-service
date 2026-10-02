# Project Tracks

This file tracks all major tracks for the project.

---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- US-5.2: consume inventory.stock-reserved / inventory.stock-reservation-failed, update order status
- US-8.1: order history read endpoint + Redis cache
- fix: reservation-failure response falls back to stale `pending` status if OrderStore.update races to None (US-4.2 review finding)
- future: per-item confirmation tracking for stock-reservation events, narrowing the blast radius of US-5.2's whole-order FIFO sku-matching (a stray event could only misattribute one line item, not flip an unrelated order's terminal status) — complements, doesn't replace, adding a real correlation id upstream (see gluon/docs/system-design.md's "Open design questions")

---
