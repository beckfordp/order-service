# Spec: Checkout Journey 1 — no order row until authorized (US-11.1, US-14.2, TD-7.3, ADR 0018)

## Overview
Reshape `POST /orders` per `gluon/docs/system-design.md` / ADR 0018: validate → mint
`orderId`/`orderItemId`s in memory → one bulk reserve → synchronous authorize → persist
order + items as `authorized` in one transaction. No row on any failure; release the
reservation on every failure after a successful reserve. Retire the per-item reserve
path, `StockEventConsumer` and `OrderReserved`.

## Functional Requirements
1. **Request**: `CreateOrderRequest` gains `shippingAddress: Address` (`line1`, `line2?`,
   `city`, `postalCode` required; `country` ISO 3166-1 alpha-2) and `paymentMethodId`
   (UUID). Own `Address` type + validation in order-service (no shared lib).
2. **Persistence**: V9 adds nullable `shipping_address JSONB` and `authorized` to the
   status CHECK. `OrderStatus` gains `Authorized`; `pending`/`reserved`/
   `reservation_failed` stay decodable for old rows but are never produced. New
   `OrderStore.createAuthorized(...)` inserts order (pre-minted ids, status
   `authorized`, address) + items in one transaction; no outbox row (payment-service
   publishes `PaymentAuthorized`).
3. **InventoryClient**: replace per-item `reserve` with `reserveOrder(orderId, items) →
   Reserved | Rejected(failures)` (201/409) and `release(orderId)` (204); other
   statuses raise.
4. **PaymentClient[F]** (new, resilience-wrapped like `InventoryClient`,
   `payment-client` config + `PAYMENT_SERVICE_URL`): `authorize(orderId, amountCents,
   paymentMethodId) → Authorized(authId, expiresAt) | Declined(reason) |
   UnknownPaymentMethod` (200/402/404); other statuses raise.
5. **Responses**: `201` `OrderResponse` (status `authorized`, + `shippingAddress`,
   `reservationFailure` field removed); `400` validation or unknown payment method;
   `402` `{error: "Payment declined", paymentFailure: {reason}}`; `409` `{error:
   "Insufficient stock", reservationFailures: [{sku, reason}]}` (every line); `503`
   `{error: "Payment service unavailable"}` on authorize raise; `503` `{error:
   "Inventory service unavailable"}` on reserve raise (new — added to contract); `500`
   if the final persist fails. All new cases are `OrderError` ADT variants mapped 1:1.
6. **Compensation**: after a successful reserve, any failure calls `release(orderId)`;
   a release failure is logged and never changes the response (ledger release is
   idempotent; orphan risk = TD-8).
7. **Retire**: `StockEventConsumer` (+ wiring, Subscribe channels), the
   `OrderReservedEvent` channel, now-unused store methods, and the scripts verifying
   removed behaviour; delete registry artifact `order-events-OrderReserved` in the same
   step. `OrderStatusChanged` status enum drops `reservation_failed`; run
   `schemaRegistryCheck` (if the registry rejects it as incompatible, keep the value and
   note it).
8. **PaymentEventConsumer**: expected current status `reserved` → `authorized` for all
   three events.

## Non-Functional Requirements
- TDD, >80% coverage, no partial functions, `sbt scalafmtCheck test` before every
  commit.
- Fail fast on an open breaker is deferred → new gluon TD (an outage costs one
  reserve+release per checkout).

## Acceptance Criteria
- Route tests cover 201/400/402/409/503×2/500 with stub clients, asserting no row on
  failure and release called exactly when required.
- `scripts/verify-checkout-journey1.sh`: real order-service + Postgres against Python
  stand-ins for inventory/payment (repo convention) — authorized, declined,
  out-of-stock, unknown method paths.
- AsyncAPI docs no longer list `StockReserved`/`StockReservationFailed`/`OrderReserved`.

## Out of Scope
- gshop changes (gshop's current `POST /orders` will 400 until its track lands — mock
  mode unaffected); purerest breaker probe; void-on-persist-failure (no endpoint);
  idempotency key (US-6.2); Journey 4/5 status refinements; TD-7.5 inventory cleanup.
