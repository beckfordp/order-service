# Plan — Adopt build-time schema registry check (Rework 5 step 3, ADR 0003)

## Phase 1: Local sbt wiring + verification against the real OrbStack Apicurio instance [checkpoint: TBD]
- [x] Task: Bump `project/plugins.sbt`'s `sbt-purekafka-docs` `0.1.0-dev`
      -> `0.2.0-dev` (now really published to GitHub Packages — pushed
      purerest `v0.3.0`, confirmed `sbt-purekafka-docs 0.2.0-dev` +
      `purekafka 0.3.0` published for real) `3bf09da`
- [x] Task: Add `schemaRegistryUrl`/`schemaRegistryGroupId` settings to
      `build.sbt` (local-dev default: OrbStack cluster-DNS address, via
      an env-var override so CI can point elsewhere; group id left at
      the plugin's own `name.value` default) `3bf09da` — also had to
      bump `purekafkaVersion` `0.2.1` -> `0.3.0` (not in the original
      task list — `SchemaRegistryWriter` didn't exist in 0.2.1, found
      via a bare `ClassNotFoundException` when first running the task)
- [x] Task: New `scripts/verify-schema-registry-check.sh` (mirroring
      `scripts/verify-kafka-asyncapi-docs.sh`), run against the real
      local Apicurio instance: registers all 6 real channels + sets the
      `BACKWARD` rule on each on first run; a second identical run is a
      no-op `3bf09da` — live-verified: all 6 real artifacts
      (`inventory-events-StockReserved`/`-StockReservationFailed`,
      `payment-events-PaymentSettled`/`-PaymentFailed`,
      `order-events-OrderReserved`/`-OrderStatusChanged`) confirmed
      present in the registry under the real `order-service` group;
      second run succeeded as a no-op
- [x] Task: Run full test suite + the verify script end-to-end; confirm
      no regressions `3bf09da` — `scalafmtCheck; Test/scalafmtCheck;
      test`: clean, exit 0; verify script: all checks passed
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Local sbt
      wiring + verification' (Protocol in workflow.md)

## Phase 2: Real CI wiring
- [ ] Task: Add a new step to `.github/workflows/ci.yml`: `docker run` a
      plain `apicurio/apicurio-registry:3.3.2` container, wait for
      `/health/ready` on port 9000, run `sbt schemaRegistryCheck` against
      it (CI-only `schemaRegistryUrl` override), tear the container down
- [ ] Task: Push this branch and confirm the new CI step passes on a
      real GitHub Actions run
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Real CI
      wiring' (Protocol in workflow.md)
