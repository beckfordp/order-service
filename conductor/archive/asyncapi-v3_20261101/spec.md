# Spec — AsyncAPI 3.0 docs and consumed-event descriptions (gluon TD-19.3/19.4)

## Overview
purekafka v0.6.0 (track `asyncapi-v3_20261101` in purerest) emits AsyncAPI 3.0 with `send`/`receive` operations from the service's own side, replacing 2.6's inverted `publish`/`subscribe`, and redirects `/docs/asyncapi` → `/docs/asyncapi/`. Produced events already carry schema descriptions and examples; consumed mirrors don't, so the docs are inconsistent.

## Functional Requirements
1. **Bump:** purekafka `0.5.0` → `0.6.0`; sbt-purekafka-docs `0.5.0-dev` → `0.6.0-dev`; tests asserting the 2.6 shape move to 3.0.
2. **Consumed-event docs:** consumed mirrors (PaymentCaptured, PaymentAuthorizationExpired, PaymentFailed, DispatchConfirmed, DispatchFailed) get `@description`/`@encodedExample` copied from the producer's latest version in gluon's `registry/<producer>/registry.json`, for the fields this mirror declares. Nothing checks the copies stay current (TD-19); descriptions don't affect the consumer dry-run.

## Acceptance Criteria
- `sbt scalafmtCheck Test/scalafmtCheck test` green; `scripts/verify-asyncapi-v3.sh` green (generated spec 3.0.0 and `asyncapi validate` clean, HTML send/receive operations, consumed descriptions, `schemaRegistryCheck` against gluon's restored registry)
- CI green on push

## Out of Scope
- Event payload changes; live redeploy check (gluon TD-19.3)
