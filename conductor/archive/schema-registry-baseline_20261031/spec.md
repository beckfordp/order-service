# Spec — Schema registry baseline from gluon's git store (gluon TD-18.5)

## Overview
gluon TD-18 makes git the schema registry's persistent store (ADR 0003, 2026-10-31 note). purekafka v0.5.0 registers producer schemas only (draft-07, closed, labelled `producer=<service>`, `BACKWARD_TRANSITIVE`) and dry-runs consumer mirrors. gluon's `registry/<producer>/registry.json` holds every version; `bin/registry-restore` replays it. Today CI's throwaway Apicurio starts empty, so `schemaRegistryCheck` never compares against anything. order-service produces `order-events-OrderConfirmed`, `order-events-OrderPaymentFailed`, `order-events-OrderCancelled`; consumes `payment-events-PaymentCaptured`, `payment-events-PaymentAuthorizationExpired`, `payment-events-PaymentFailed`, `fulfilment-events-DispatchConfirmed`, `fulfilment-events-DispatchFailed`.

## Functional Requirements
1. purekafka `0.3.0` → `0.5.0`; sbt-purekafka-docs `0.4.0-dev` → `0.5.0-dev` (producer label defaults to `name.value` = `order-service`).
2. CI: sparse-checkout `beckfordp/gluon` (`registry/`, `bin/registry-restore`) and restore it into the throwaway Apicurio before `schemaRegistryCheck`.
3. `scripts/verify-schema-registry-check.sh`: restore the baseline locally, run the check, assert produced artifacts are labelled `order-service`, consumed ones untouched, rerun adds no versions.

## Acceptance Criteria
- `sbt scalafmtCheck Test/scalafmtCheck test` green; verify script green against local Apicurio
- CI green on push

## Out of Scope
- Event payload changes; gluon export commit (done in gluon)
