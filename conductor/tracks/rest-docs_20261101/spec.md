# Spec: REST docs convention (gluon TD-20.3)

## Overview
order-service `/docs` shows bare paths and unexplained schemas. Apply the TD-20 convention (purerest README "Documenting endpoints") and assert it in a test.

## Functional Requirements
- FR1: purerestlib 0.4.0 → 0.7.3 (+ aligned pins); suite green.
- FR2: Per-endpoint error outputs — variants defined once, each endpoint's `oneOf` lists only what it can return: POST 400×3 (empty items / invalid item / invalid checkout), 409 insufficient stock, 402, 503×2, 500; GET list none; GET by id 404; PATCH/PUT 404, 400 invalid status, 409 illegal transition; DELETE 404.
- FR3: Every endpoint `.summary`/`.description`/`.tag` (`orders`, `health`) — see Appendix.
- FR4: `id`, `customerId` params described + example.
- FR5: Every request/response DTO + field `@description`; each class `@encodedExample` (incl. Address, error bodies).
- FR6: Every error variant described + example body.
- FR7: `OrderDocsSuite`: `Docs.undocumented(all endpoints) == Nil`.
- FR8: `scripts/verify-rest-docs.sh` checks served `/docs/docs.yaml` for summaries/descriptions/examples.

## Non-Functional
- Wire format unchanged (encode/decode identical); scalafmtCheck clean.
- No real ids/credentials in examples.

## Acceptance Criteria
- Full suite green, zero gaps, verify script PASS, CI green.
- `/docs` no longer lists impossible errors per endpoint.

## Out of Scope
- Client mirror DTOs (InventoryClient/PaymentClient).
- Behaviour changes; AsyncAPI side (TD-19).

## Appendix: approved descriptions (2026-11-01)

### Endpoints
| Endpoint | summary | description |
|---|---|---|
| POST /orders | Place an order | Checkout: validates items/address/payment method, reserves stock for all lines, authorizes payment, persists as `authorized`. Nothing persisted unless every step succeeds; reserved stock released on failure. Not idempotent — each call is a new order. |
| GET /orders | List a customer's orders | Customer's order history, newest first. Cache-backed (short TTL) — a just-placed order may lag. |
| GET /orders/{id} | Get an order | One order with its line items. |
| PATCH /orders/{id} | Cancel an order | Only `cancelled` accepted; publishes `OrderCancelled`. Other valid statuses → 409. |
| PUT /orders/{id} | Cancel an order (replace) | Same as PATCH; kept for PUT clients. |
| DELETE /orders/{id} | Delete an order | Hard-deletes order + items. No event published. |
| GET /health | Liveness | 200 while process up. |
| GET /health/ready | Readiness | 200 when Postgres reachable, else 503. |

### Parameters
- `id`: "Order id (UUID)", ex `3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90`
- `customerId`: "Customer whose orders to list", ex `cust-42`

### DTO fields
- CreateOrderRequest: customerId "Customer placing the order"; items "Lines to buy, at least one"; shippingAddress "Where to ship"; paymentMethodId "Saved payment method (UUID) to authorize against"
- CreateOrderItemRequest: sku "Catalog SKU" (`SKU-MUG-001`); productName "Name shown on the order" (`Gluon Mug`); unitPriceCents "Unit price in cents, ≥0" (`1299`); quantity "Units, >0" (`2`)
- UpdateOrderRequest.status: "Target status; only `cancelled` accepted" (`cancelled`)
- OrderResponse: id; customerId; totalCents "Sum of line totals, cents"; status "Lifecycle status" (`authorized`); items; createdAt/updatedAt "UTC timestamp"; shippingAddress "Absent on pre-US-11.1 orders"
- OrderItemResponse: id "Line id (UUID)" + request-item fields
- Address: line1, line2 "Optional", city, postalCode, country "ISO 3166-1 alpha-2" (`GB`)
- ErrorResponse.error: "Human-readable reason"
- PaymentDeclinedResponse / PaymentFailure.reason: "Why the provider declined" (`insufficient_funds`)
- InsufficientStockResponse.reservationFailures / ReservationFailureResponse: sku, reason (`out_of_stock`)

### Error variants
| Status | When | Example body |
|---|---|---|
| 404 | No order with that id | `{"error":"Order not found"}` |
| 400 | `status` not a known status | `{"error":"Invalid order status: 'shipped!'"}` |
| 400 | `items` empty | `{"error":"Order must have at least one item"}` |
| 400 | line quantity ≤0 or price <0 | `{"error":"quantity must be positive, got 0 for sku 'SKU-MUG-001'"}` |
| 400 | bad address, paymentMethodId not UUID / unknown, total overflow | `{"error":"paymentMethodId must be a UUID"}` |
| 409 | status can't move to `cancelled` / non-`cancelled` target | `{"error":"Illegal status transition to 'dispatched'"}` |
| 409 | some lines couldn't be reserved | `{"error":"Insufficient stock","reservationFailures":[…]}` |
| 402 | payment declined | `{"error":"Payment declined","paymentFailure":{"reason":"insufficient_funds"}}` |
| 503 | payment-service unreachable | `{"error":"Payment service unavailable"}` |
| 503 | inventory-service unreachable | `{"error":"Inventory service unavailable"}` |
| 500 | authorized but persist failed | `{"error":"Order could not be saved"}` |
