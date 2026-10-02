# Project Tracks

This file tracks all major tracks for the project.

---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- US-5.3: publish order.reserved once an order's stock is fully reserved (payload/trigger pinned 2026-10-02 in gluon/docs/system-design.md; needs a Kafka producer — order-service has none yet, consumer-only so far; publish wrapped in purerest.resilience's bounded retry, log-and-drop on exhaustion, no outbox — see system-design.md's "Payload contracts" reliability note)
- US-5.4: publish order.status-changed (reservation_failed) when a reservation fails, from either OrderRoutes' synchronous checkout-failure path or StockEventConsumer's reservationFailedStream — closes a gap found 2026-10-02 (system-design.md decided order-service publishes this, but no task ever assigned the work); reuses the same OrderEventPublisher infra as US-5.3; payload: orderId, customerId, status="reservation_failed", timestamp
- US-6.3: consume payment.settled / payment.failed, update order status to confirmed / payment_failed, publish order.status-changed (new OrderStatus cases + DB migration needed; depends on payment-service's US-6.1 actually publishing, but can be built/tested now against synthetic events per the usual stub/fan-out pattern)
- fix: reservation-failure response falls back to stale `pending` status if OrderStore.update races to None (US-4.2 review finding)
- future: surface StockEventConsumer startup/fiber failure (e.g. Kafka unreachable) via /health/ready or a distinct error log, instead of the server silently running with no consumer (US-5.2 review finding)

---
