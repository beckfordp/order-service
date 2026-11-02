# Specification: US-9.2 — `reason` on `OrderCancelled`

## Overview

`OrderCancelled` currently carries no reason, so consumers cannot tell a
customer cancellation from an authorization expiry (notification always says
"authorization expired"). Checkout Journey 5's expiry path and US-9's shared
cancellation fan-out both need the trigger. Add an optional `reason` property,
always populated by this producer: `customer` from `PATCH`/`PUT /orders/{id}`,
`auth_expired` from the `PaymentAuthorizationExpired` consumer. `stale` and
`operator` are reserved for later journeys.

## Functional Requirements

- FR-1: `OrderCancelledEvent` gains `reason: Option[String] = None`, with a
  description and example in the tapir schema.
- FR-2: the customer-cancel path publishes `Some("customer")`.
- FR-3: the `PaymentAuthorizationExpired` path publishes
  `Some("auth_expired")`.
- FR-4: the property is optional in the registry schema so the
  `BACKWARD_TRANSITIVE` rule accepts it; the producer always sends it.
- FR-5: tests assert both publish sites carry their reason.

## Non-Functional Requirements

- `sbt scalafmtCheck test` green; no behaviour change beyond the new field.
- Registry: `sbt schemaRegistryCheck` registers the new backward-compatible
  version; `bin/registry-export` updates the git store.

## Acceptance Criteria

- A customer cancel publishes `OrderCancelled {reason: "customer"}`.
- An expiry cancellation publishes `OrderCancelled {reason: "auth_expired"}`.
- Existing consumers that omit the field still decode it (`None`).

## Out of Scope

Consumer mirrors (separate tracks), notification copy (US-9.9), US-21/US-22.
