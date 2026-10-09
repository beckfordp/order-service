# Spec: Surface which sku failed reservation in OrderResponse

## Overview
Add a `reservationFailure: Option[{sku, reason}]` field to `OrderResponse`, populated from
the already-computed per-item failure reason in `OrderRoutes.reserveAll` during checkout
(`POST /orders`), so API clients can tell which sku caused a `reservation_failed` status
and why — instead of only seeing the aggregate status.

## Functional Requirements
- New case class `ReservationFailure(sku: String, reason: String)` with a circe `Codec`.
- `OrderResponse` gains `reservationFailure: Option[ReservationFailure]` (default `None`),
  serialized as JSON `null` when absent.
- `reserveAll` changes its failure channel from `Either[String, Unit]` to
  `Either[ReservationFailure, Unit]`, carrying `sku` explicitly instead of only
  interpolating it into a message string:
  - `"insufficient stock for sku '<sku>'"` for `InsufficientStock`
  - `"unknown sku '<sku>'"` for `UnknownSku`
  - `"reservation call failed for sku '<sku>': <exception message>"` verbatim for a
    raised error (retries exhausted, circuit breaker open, etc.) — exposed as-is, no
    sanitization
- The checkout handler (`OrderRoutes.serverEndpoint`) populates `reservationFailure` with
  that structured failure only in the synchronous-failure branch of the very
  `POST /orders` call that failed. The existing `logger.warn` content is unchanged.
- All other response paths (`GET /orders/{id}`, `GET /orders?customerId=`, PATCH/PUT, and
  the existing stale-order fallback for the known US-4.2 race) continue to produce
  `reservationFailure = None` — not persisted, so a later GET of a `reservation_failed`
  order can't recover it. Ephemeral/sync-response-only by design.
- The async `StockEventConsumer` (Kafka) path is unaffected — it doesn't build
  `OrderResponse` and is out of scope.

## Non-Functional Requirements
- No DB migration.
- `reservation_failed` orders still return HTTP `201 Created` (unchanged) —
  `reservationFailure` just adds detail inside that body.
- Additive/backward-compatible: existing clients ignoring unknown JSON fields are
  unaffected.

## Acceptance Criteria
- `POST /orders` failing due to insufficient stock, unknown sku, or a reservation-client
  error returns `201` with `status: "reservation_failed"` and
  `reservationFailure: {sku, reason}` for the specific failing item.
- `POST /orders` that reserves successfully returns `reservationFailure: null`.
- `GET /orders/{id}` and `GET /orders?customerId=` always return
  `reservationFailure: null`, even for `reservation_failed` orders.
- Test coverage added for all three failure cases (insufficient stock / unknown sku /
  call failure), each asserting the expected `sku`/`reason` pair.

## Out of Scope
- Persisting the failure reason (no DB column) — later GETs won't show it.
- Populating `reservationFailure` for orders flipped via the async
  StockEventConsumer/Kafka path.
- Sanitizing the raw exception message in the call-failure case.
- The existing US-4.2 stale-`pending`-fallback bug, and CORS support — separate backlog
  items, untouched here.
