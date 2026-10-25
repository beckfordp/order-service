# Spec — Adopt build-time schema registry check (Rework 5 step 3, ADR 0003)

## Overview
Wire order-service's existing `AsyncApiChannels` registrations into
`sbt-purekafka-docs`'s new `schemaRegistryCheck` task (purerest `v0.3.0`/
`sbt-purekafka-docs 0.2.0-dev`, now really published to GitHub Packages), so
each of this service's 6 registered Kafka event schemas is checked/
registered against Apicurio Registry — locally against the real OrbStack
instance, and in real CI via an ephemeral container.

## Functional Requirements
- Bump `project/plugins.sbt`'s `sbt-purekafka-docs` version `0.1.0-dev` ->
  `0.2.0-dev`.
- Add `schemaRegistryUrl` (local-dev default: the OrbStack cluster-DNS
  address, `http://apicurio-registry.gluon-local.svc.cluster.local:8080`)
  and `schemaRegistryGroupId` (keep the plugin's own default, `name.value`
  = `"order-service"`) settings to `build.sbt`.
- Add a new CI step to `.github/workflows/ci.yml`: `docker run` a plain
  `apicurio/apicurio-registry:3.3.2` container, wait for its
  `/health/ready` on port 9000, run `sbt schemaRegistryCheck` against
  `http://localhost:8080` (overriding `schemaRegistryUrl` via a CI-only
  setting or env, since the local-dev default points at OrbStack which CI
  can't reach), tear the container down.
- New `scripts/verify-schema-registry-check.sh` (mirroring
  `scripts/verify-kafka-asyncapi-docs.sh`'s structure) proving the task
  works end-to-end locally against the real OrbStack Apicurio instance,
  registering all 6 of this service's actual channels (not a throwaway
  fixture, unlike purerest's own track — this one exercises the real
  production registrations).

## Non-Functional Requirements
- No change to runtime behavior — this is build/CI-time only, consistent
  with ADR 0003's zero-runtime-presence design.
- CI's ephemeral Apicurio instance is fully separate from the shared local
  OrbStack one — no shared state, no risk of CI runs polluting local-dev
  schema history.

## Acceptance Criteria
- `sbt schemaRegistryCheck` succeeds locally against the real OrbStack
  Apicurio instance, registering all 6 channels (`StockReserved`,
  `StockReservationFailed`, `PaymentSettled`, `PaymentFailed`,
  `OrderReserved`, `OrderStatusChanged`).
- The new CI step passes on a real GitHub Actions run (push to a
  branch/PR) using the ephemeral `docker run` Apicurio container.
- Full existing test suite + the new verify script run clean, no
  regressions.

## Out of Scope
- inventory-service/payment-service/notification-service adoption (their
  own, separate tracks).
- `pure-service-generator` template retrofit (Rework 5 step 4, separate
  track).
- Any change to the Debezium CDC / outbox publish path.
