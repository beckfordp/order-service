# Specification: ADM-10 — guest checkout email

## Overview

Anonymous gshop orders use a random customerId with no user-service account,
so notification records them `unresolved` (ADR 0025). Capture an optional
guest email at checkout and expose it for consume-time resolution.

## Functional Requirements

- `CreateOrderRequest.email` (optional): blank = absent; trimmed,
  lower-cased, `local@domain.tld` shape (`GuestEmail.normalize`), else 400
  before any reserve.
- `V16__add_order_guest_email.sql`: nullable `guest_email`; persisted by
  `createAuthorized`, read by `get`/`listByCustomer`/`listAll`.
- `OrderResponse.guestEmail` (optional) on every order read, so
  notification's existing `GET /orders/{id}` lookup sees it.

## Non-Functional
- TDD; `sbt scalafmtCheckAll test` green. No Kafka contract change (ADR 0025
  lookup at consumption).

## Out of Scope
- Notification fallback and gshop field (sibling ADM-10 tracks).
