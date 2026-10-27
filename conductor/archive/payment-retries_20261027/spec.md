# Specification: TD-10.2 — restore payment-client retries

## Overview

order-service's `payment-client` has `max-retries = 0` until
`POST /payments/authorize` became idempotent per order (TD-10.1). A retry
whose first attempt landed would have placed a second hold. TD-10.1 now
replays the stored authorization by `orderId`, so retries are safe again.

## Functional Requirements

- **FR-1** — `payment-client.resilience.retry.max-retries` returns to `3`
  with a comment pointing at TD-10.1's idempotency.
- **FR-2** — No other resilience settings change.
- **FR-3** — The existing `PaymentClientSuite` retry/circuit-breaker tests
  remain green; the config suite still parses a retried payment-client.

## Acceptance Criteria

- `sbt scalafmtCheck test` green.
- Live order-service image carries `max-retries = 3` for `payment-client`
  and checkout still succeeds (gshop's live checkout script).

## Out of Scope

- TD-9 (fail-fast on an open payment breaker).
- Any change to the authorize contract itself.
