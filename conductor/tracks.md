# Project Tracks

This file tracks all major tracks for the project.

---

- [ ] **Track: Surface which sku failed reservation in OrderResponse**
  *Link: [./tracks/reservation-failure-detail_20261009/](./tracks/reservation-failure-detail_20261009/)*

---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- fix: reservation-failure response falls back to stale `pending` status if OrderStore.update races to None (US-4.2 review finding)
- future: surface StockEventConsumer startup/fiber failure (e.g. Kafka unreachable) via /health/ready or a distinct error log, instead of the server silently running with no consumer (US-5.2 review finding)
- Add CORS support (no `Access-Control-Allow-Origin` header today) — same
  gap as catalog-service/cart-service (see `backlogs/catalog-service.md`),
  confirmed 2026-10-09 verifying gshop's US-3 Checkout button: `curl` gets
  a clean 201 from `POST /orders` with no ACAO header even with an
  `Origin` request header sent, but every browser blocks gshop's
  `fetch()`. Worked around for local dev with a third Vite dev-server
  proxy entry in gshop (`frontends/gshop/vite.config.ts`'s
  `server.proxy['/api/order']`, dev-only). Likely the same generator-level
  gap — check `pure-service-generator` first rather than patching each
  service individually.

---
