# Plan: Rework 7 order-service — sync-payment-auth Kafka contracts

## Phase 1: Event mirrors, consumer transitions, Cancelled status (TDD)

- [ ] Task: **Red** — update tests first: `AsyncApiChannelsSuite` expects 7 channels (2 inventory
      consumes, `PaymentCaptured`/`PaymentAuthorizationExpired`/`PaymentFailed` consumes, 2 order
      CDC publishes; no `PaymentSettled`/`PaymentAuthorized`); replace `PaymentSettled` cases in
      `PaymentEventConsumerSuite` with `PaymentCaptured` (→`Confirmed` + outbox `confirmed`) and
      `PaymentAuthorizationExpired` (→`Cancelled` + outbox `cancelled`), plus
      wrong-state/unknown-id no-ops and the new `PaymentFailed.reason` shape; `OrderStatusSuite`
      adds `Cancelled` parse/print; `MigrationsSuite` expects the CHECK to accept `cancelled`;
      `OrderAsyncApiDocsSuite` expects 7/updated enum. Run `sbt test`; confirm expected failures.
- [ ] Task: **Green** — implement: add `OrderStatus.Cancelled`; new migration (`V8`) replacing the
      status CHECK to include `cancelled`; update local payment mirrors (delete
      `PaymentSettledEvent`, add `PaymentCapturedEvent`/`PaymentAuthorizationExpiredEvent`, add
      `reason` to `PaymentFailedEvent`); add `handleCaptured`/`handleExpired` dispatch branches
      with guarded `Reserved` transitions and outbox rows; add `cancelled` to
      `OrderStatusChangedEvent`'s enum; update `AsyncApiChannels` to the 7-channel set. Run
      `sbt test` green.
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Event mirrors, consumer transitions,
      Cancelled status' — prompting is off (`.claude/prompt-off`); verify via `sbt scalafmt test`
      green and `sbt asyncApiGenerate` showing the 7 channels.

## Phase 2: Docs, scripts, live Kafka verification + schema registry

- [ ] Task: Update order-service docs — `README.md` consumer/event lists, `conductor/product.md`
      (`PaymentEventConsumer`, events section, `OrderStatus` five→six values),
      `conductor/tech-stack.md` (payment-events consumptions). Update
      `scripts/verify-kafka-asyncapi-docs.sh` (7 event types, no `PaymentSettled`), retarget
      `scripts/verify-payment-settlement-consumer-end-to-end.sh` to capture/expiry (rename to
      `verify-payment-outcome-consumers-end-to-end.sh`), fix
      `scripts/verify-schema-registry-check.sh` (payment artifacts list + `gluon` group).
- [ ] Task: Run `sbt asyncApiGenerate schemaRegistryCheck` against the local Apicurio `gluon`
      group — `PaymentCaptured`/`PaymentAuthorizationExpired` register; `PaymentFailed` matches
      payment-service's new shape; then run the updated AsyncAPI + payment-consumer end-to-end
      scripts against a real stack. Record any registry-compatibility surprises (the
      PaymentFailed artifact was already reset by payment-service's half).
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Docs, scripts, live Kafka verification
      + schema registry' — run `sbt test`, `sbt asyncApiGenerate`, `sbt schemaRegistryCheck`, and
      the updated verification scripts; confirm clean results.

## Phase 3: Commit + root checkoff

- [ ] Task: Commit and push order-service changes; in gluon `PLAN.md` tick Rework 7's four bullets
      and mark the section DONE (payment + order halves complete), fix any remaining Rework 7
      references (e.g. diagrams/ADR pointers if cheap), and commit/push gluon.
- [ ] Task: Conductor - User Manual Verification 'Phase 3: Commit + root checkoff' — confirm clean
      trees, pushed commits, and Rework 7 marked done in `PLAN.md`.
