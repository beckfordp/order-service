# Overview
Harden `Order.status` from a raw String to a closed Scala ADT (`OrderStatus`:
Pending, Reserved, ReservationFailed), matching OrderError's existing ADT
style. The Postgres `status` column stays `text` (no native enum type) with
a new CHECK constraint enforcing the same closed set at the DB level; a
`Codec[OrderStatus]` built via `text.eimap` handles parsing/validation on the
Scala side. The external JSON wire format is unchanged — status still
appears as a lowercase string ("pending"/"reserved"/"reservation_failed") in
requests/responses; only the internal representation and DB constraint
change.

# Functional Requirements
- New `OrderStatus` sealed trait (Pending/Reserved/ReservationFailed) with
  `fromString(String): Either[String, OrderStatus]` and `asString: String`
- New `InvalidStatus(raw: String)` case added to the `OrderError` ADT
- `OrderRoutes`: PATCH/PUT parse the raw status string via
  `OrderStatus.fromString` before calling the store; invalid values
  short-circuit to `Left(InvalidStatus(raw))`, mapped to 400 via a new
  explicit errorOut case (alongside the existing `notFoundOutput`)
- `OrderStore.update` signature changes from `status: String` to
  `status: OrderStatus`; `Order.status` becomes `OrderStatus`
- `OrderResponse` keeps `status: String` in JSON — built via
  `OrderStatus.asString`
- New Flyway migration adding
  `CHECK (status IN ('pending','reserved','reservation_failed'))` to the
  existing `order` table
- New Skunk codec
  `val orderStatus: Codec[OrderStatus] = text.eimap(OrderStatus.fromString)(_.asString)`,
  used in insert/select/update queries in place of raw `text`
- In-memory `OrderStore` updated to use `OrderStatus` too

# Non-Functional Requirements
- No change to the external JSON wire format
- No partial functions / exhaustive pattern matches throughout (per
  development-guidelines.md)

# Acceptance Criteria
- [ ] `OrderStatus` ADT added with `fromString`/`asString`
- [ ] `OrderError.InvalidStatus` added, mapped to 400 via a dedicated
  errorOut case
- [ ] PATCH/PUT with an invalid status string returns 400, nothing persisted
- [ ] PATCH/PUT with a valid status string behaves exactly as before (wire
  format unchanged)
- [ ] New Flyway migration adds the CHECK constraint, applies cleanly on top
  of V1
- [ ] Skunk queries use the new eimap-based codec for `status`, not raw
  `text`
- [ ] `sbt scalafmtCheck test` passes, including new tests for
  `OrderStatus.fromString` and invalid-status PATCH/PUT behavior

# Out of Scope
- Restricting which statuses a client may set via PATCH/PUT (deferred to
  US-5.2)
- `order_items` table / FK (separate backlog items)
- Any change to OrderResponse's JSON shape
