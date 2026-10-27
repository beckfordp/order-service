# Plan: TD-10.2 — restore payment-client retries

## Phase 1: Config + verification

- [x] Task: Set `payment-client.resilience.retry.max-retries = 3` in `application.conf`, replacing the TD-10 comment with a TD-10.1 idempotency note (Green) `79daa50`
- [x] Task: `sbt scalafmtCheck test` green `79daa50` (170/170 tests; shipped-config assertion updated)
- [x] Task: Rebuild/redeploy order-service; run gshop's `scripts/verify-checkout-real.sh` to prove checkout is unaffected — live run green (201/402/409/400)
- [x] Task: Conductor - User Manual Verification 'Config + verification' (Protocol in workflow.md) — live checkout script green against the redeployed cluster

## Risks / notes

- Retrying a 5xx now reuses the same `orderId`, so payment-service returns the stored authorization instead of holding twice.
- Authorize declines are not retried by the client (only transport/5xx failures are), unchanged from the inventory-client shape.
