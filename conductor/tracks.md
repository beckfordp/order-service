# Project Tracks

This file tracks all major tracks for the project.

---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- No CORS support (no `Access-Control-Allow-Origin` header) — confirmed
  2026-10-09 verifying gshop's US-3 Checkout button: `curl` gets a clean
  201 with no ACAO header, but every browser blocks gshop's `fetch()`.
  Likely a `pure-service-generator`-level gap (same template as
  catalog/cart/inventory/payment/notification) — fix there first
  (`src/main/g8/src/main/scala/$package$/Main.scala`, wrap `routes`
  with http4s's `org.http4s.server.middleware.CORS` before
  `.orNotFound`), then port here. See `gluon/TECHNICAL_DEBT.md` TD-1 and
  `gluon/backlogs/order-service.md`.
- fix: reservation-failure response falls back to stale `pending` status if OrderStore.update races to None (US-4.2 review finding)
- future: surface StockEventConsumer startup/fiber failure (e.g. Kafka unreachable) via /health/ready or a distinct error log, instead of the server silently running with no consumer (US-5.2 review finding)

---
