# Plan: Surface which sku failed reservation in OrderResponse

## Phase 1: Add `reservationFailure` to OrderResponse

- [x] Task: Write failing tests (Red) [9776333]
  - [x] Add cases to `OrderRoutesSuite.scala` asserting `POST /orders` returns
        `reservationFailure: {sku, reason}` for: insufficient stock, unknown sku, and a
        raised reservation-call error (circuit-breaker/retry failure)
  - [x] Add a case asserting a fully-successful `POST /orders` returns
        `reservationFailure: null`
  - [x] Add/extend a case asserting `GET /orders/{id}` for an existing
        `reservation_failed` order returns `reservationFailure: null` (ephemeral, not
        persisted)
  - [x] Run `sbt test`, confirm the new cases fail (Red) — confirmed as 8 compile
        errors (`ReservationFailure`/`reservationFailure` not found), as expected in
        Scala's statically-typed Red phase

- [x] Task: Implement to pass tests (Green) [7f19542]
  - [x] Add `ReservationFailure(sku: String, reason: String)` case class + circe `Codec`
        in `OrderRoutes.scala`
  - [x] Add `reservationFailure: Option[ReservationFailure]` field to `OrderResponse`
        (default kept on the custom `entity`/`items` `apply` overload instead of the
        case-class field itself — Scala disallows default args on two overloaded
        `apply` methods at once; `OrderHistoryCacheSuite.scala`, the one call site using
        the generated 8-arg `apply`, updated to pass `reservationFailure = None`
        explicitly)
  - [x] Change `reserveAll`'s return type from `F[Either[String, Unit]]` to
        `F[Either[ReservationFailure, Unit]]`, constructing
        `ReservationFailure(item.sku, <message>)` for each of the three failure branches
        (message text unchanged from today, including the verbatim exception message in
        the call-failure case)
  - [x] Update the checkout handler in `serverEndpoint` to: keep the existing
        `logger.warn` log line using `failure.reason`, and populate the final
        `OrderResponse`'s `reservationFailure` with `failureOpt` (derived from
        `reservation` independently of the US-4.2 stale-order fallback) only on this
        synchronous-failure branch
  - [x] Run `sbt test`, confirm all tests pass (Green) — 134 total, 0 failed, full suite

- [x] Task: Refactor (optional)
  - [x] Reviewed `reserveAll` and the checkout handler for clarity — structured
        `ReservationFailure` reads cleanly, no duplication introduced; no changes made

- [x] Task: Verify coverage
  - [x] Run `sbt coverage test coverageReport` — 92.68% statement, 93.55% branch
        (project-wide), well above the 80% target

- [ ] Task: Conductor - User Manual Verification 'Add reservationFailure to
      OrderResponse' (Protocol in workflow.md)
