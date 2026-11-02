# Plan: US-9.2 — `reason` on `OrderCancelled`

## Phase 1: Event + publish sites

- [x] Task: Failing tests — customer cancel publishes `Some("customer")`; expiry consumer publishes `Some("auth_expired")` (Red)
- [x] Task: Add `reason: Option[String]` to `OrderCancelledEvent`; populate both call sites (Green)
- [x] Task: Refactor, `sbt scalafmtCheck test` green
- [x] Task: Conductor - User Manual Verification 'Event + publish sites' (Protocol in workflow.md)

## Phase 2: Registry + docs

- [x] Task: `sbt schemaRegistryCheck`; `bin/registry-export`; update `system-design.md`
- [x] Task: Conductor - User Manual Verification 'Registry + docs' (Protocol in workflow.md)

## Risks

- Required-in-schema addition would fail BACKWARD_TRANSITIVE; keep it optional.

## Review (2026-11-02)

- `sbt scalafmtCheck test` green (201 tests); `sbt schemaRegistryCheck` green.
- Live path covered by `services/payment-service/scripts/verify-auth-expiry.sh`.
- `OrderCancelled {reason: auth_expired}` observed end-to-end; `reason` optional
  in registry v2 so BACKWARD_TRANSITIVE holds. No blocking findings.
