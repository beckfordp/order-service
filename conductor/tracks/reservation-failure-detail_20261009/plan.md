# Plan: Surface which sku failed reservation in OrderResponse

## Phase 1: Add `reservationFailure` to OrderResponse

- [ ] Task: Write failing tests (Red)
  - [ ] Add cases to `OrderRoutesSuite.scala` asserting `POST /orders` returns
        `reservationFailure: {sku, reason}` for: insufficient stock, unknown sku, and a
        raised reservation-call error (circuit-breaker/retry failure)
  - [ ] Add a case asserting a fully-successful `POST /orders` returns
        `reservationFailure: null`
  - [ ] Add/extend a case asserting `GET /orders/{id}` for an existing
        `reservation_failed` order returns `reservationFailure: null` (ephemeral, not
        persisted)
  - [ ] Run `sbt test`, confirm the new cases fail (Red)

- [ ] Task: Implement to pass tests (Green)
  - [ ] Add `ReservationFailure(sku: String, reason: String)` case class + circe `Codec`
        in `OrderRoutes.scala`
  - [ ] Add `reservationFailure: Option[ReservationFailure] = None` field to
        `OrderResponse`, update its codec/`apply`
  - [ ] Change `reserveAll`'s return type from `F[Either[String, Unit]]` to
        `F[Either[ReservationFailure, Unit]]`, constructing
        `ReservationFailure(item.sku, <message>)` for each of the three failure branches
        (message text unchanged from today, including the verbatim exception message in
        the call-failure case)
  - [ ] Update the checkout handler in `serverEndpoint` to: keep the existing
        `logger.warn` log line using `failure.reason`, and populate the final
        `OrderResponse`'s `reservationFailure = Some(failure)` only on this
        synchronous-failure branch
  - [ ] Run `sbt test`, confirm all tests pass (Green)

- [ ] Task: Refactor (optional)
  - [ ] Review `reserveAll` and the checkout handler for clarity now that the failure
        channel is structured; rerun tests after any change

- [ ] Task: Verify coverage
  - [ ] Run `sbt coverage test coverageReport`, confirm new code ≥ 80%

- [ ] Task: Conductor - User Manual Verification 'Add reservationFailure to
      OrderResponse' (Protocol in workflow.md)
