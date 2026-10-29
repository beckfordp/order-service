# Project Tracks

This file tracks all major tracks for the project.

- [ ] **Track: TD-15.1 — Cross-repo consistency hardening (order-service)**
  *Link: [./tracks/td15-order-hardening_20261029/](./tracks/td15-order-hardening_20261029/)*

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- Pure order domain core: OrderTransitions state machine (single source for consumers, stores, PATCH), OrderId opaque type parsed at edge, typed Either results instead of Option/wildcards
- Purity refactor: split domain/ vs store/ packages (pure Order.create/total), narrow F[_]: Async to Monad/Clock/Temporal where sufficient

---
