# Plan — Adopt build-time schema registry check (Rework 5 step 3, ADR 0003)

## Phase 1: Local sbt wiring + verification against the real OrbStack Apicurio instance
- [ ] Task: Bump `project/plugins.sbt`'s `sbt-purekafka-docs` `0.1.0-dev`
      -> `0.2.0-dev` (now really published to GitHub Packages)
- [ ] Task: Add `schemaRegistryUrl`/`schemaRegistryGroupId` settings to
      `build.sbt` (local-dev default: OrbStack cluster-DNS address; group
      id left at the plugin's own `name.value` default)
- [ ] Task: New `scripts/verify-schema-registry-check.sh` (mirroring
      `scripts/verify-kafka-asyncapi-docs.sh`), run against the real
      local Apicurio instance: registers all 6 real channels + sets the
      `BACKWARD` rule on each on first run; a second identical run is a
      no-op
- [ ] Task: Run full test suite + the verify script end-to-end; confirm
      no regressions
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
