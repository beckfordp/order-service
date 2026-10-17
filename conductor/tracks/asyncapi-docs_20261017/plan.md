# Plan — Migrate order-service to purekafka AsyncAPI docs (ADR 0010)

## Phase 1 — Dependency + schema derivation [checkpoint: 1003a17]
- [x] Task: Write failing test asserting `AsyncApiChannel[StockReservedEvent]`/etc. construct (needs an implicit tapir `Schema[A]` for each of the 4 event case classes) (Red) `a06b7a8`
- [x] Task: Bump `io.github.beckfordp:purekafka` to the post-fix version; add tapir `Schema[A]` derivation for `StockReservedEvent`, `StockReservationFailedEvent`, `PaymentSettledEvent`, `PaymentFailedEvent` (Green) `a06b7a8` — committed Red+Green together; bumped to `0.1.0+71-2ff60583`, which includes purerest's `asyncapi-topic-merge_20261017` fix
- [x] Task: Conductor - User Manual Verification 'Phase 1: Dependency + schema derivation' (Protocol in workflow.md) `1003a17` — reduced-prompting mode: automated-test verification only (148 passed), no interactive walkthrough; see git note on `1003a17`

## Phase 2 — Channel registration + raw-spec route [checkpoint: baea2ad]
- [x] Task: Write failing test: a new `AsyncApiChannels` registry exposes all 4 channels with correct topic/eventType pairs (Red) `c2cd062`
- [x] Task: Implement the registry (Green) `c2cd062` — committed Red+Green together
- [x] Task: Write failing test: `GET /docs/asyncapi.json` returns 200 with a document containing all 4 event types across both topics (Red) `0db4e39` — deviation: already-Green, not Red; see git note on `0db4e39`
- [x] Task: Mount `purekafka.docs.Docs.routes[F]` in `Main.scala`, merged with the existing OpenAPI routes (Green) `bd4d17e`
- [x] Task: Conductor - User Manual Verification 'Phase 2: Channel registration + raw-spec route' (Protocol in workflow.md) `baea2ad` — reduced-prompting mode: automated-test verification only (150 passed), no interactive walkthrough; see git note on `baea2ad`

## Phase 3 — Static HTML generation + serving [checkpoint: ca0203f]
- [x] Task: Adapt `scripts/generate-kafka-asyncapi-docs.sh`/`verify-kafka-asyncapi-docs.sh` from purerest for order-service (own port/route, same version pins + patched-template workaround) `e5f046f`
- [x] Task: Add a second `Main.scala` route (e.g. `/docs/asyncapi`) serving the generated static HTML, sibling to the raw-spec route `b570e83`
- [x] Task: Conductor - User Manual Verification 'Phase 3: Static HTML generation + serving' (Protocol in workflow.md) `ca0203f` — reduced-prompting mode: end-to-end script verification only (`./scripts/verify-kafka-asyncapi-docs.sh`, all checks passed); found and fixed a route-ordering bug along the way (`cdcf77c`); see git note on `ca0203f`

## Phase 4 — Docs + full verification [checkpoint: 901dad2]
- [x] Task: Update order-service's README with a "View Kafka AsyncAPI docs" section `282ec48`
- [x] Task: Run full test suite + the adapted verify script end-to-end; confirm no regressions — 150 tests passing, `scalafmtCheck` clean, `./scripts/verify-kafka-asyncapi-docs.sh` passes end-to-end (all 4 event types in both raw spec and served HTML); no code changes, nothing to commit for this task
- [x] Task: Conductor - User Manual Verification 'Phase 4: Docs + full verification' (Protocol in workflow.md) `901dad2` — reduced-prompting mode: automated/end-to-end script verification only (150 passed, verify script passed), no interactive walkthrough; see git note on `901dad2`
