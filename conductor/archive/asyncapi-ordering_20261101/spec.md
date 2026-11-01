# Spec — AsyncAPI docs: sends first (purekafka 0.6.1) (gluon TD-19.6)

## Overview
html-template renders AsyncAPI operations in document order, which followed channel registration, so a service that registers a consumed topic first opened its docs with `RECEIVE`. purekafka v0.6.1 (purerest track `asyncapi-ordering_20261101`) lists every `send` before every `receive`, registration order within each group; channels follow the same order.

## Functional Requirements
1. **Bump:** purekafka `0.6.0` → `0.6.1`; sbt-purekafka-docs `0.6.0-dev` → `0.6.1-dev`; pin comments and `conductor/tech-stack.md` updated.
2. **Verify:** `scripts/verify-asyncapi-v3.sh` asserts no `receive` operation precedes a `send` (skipped where the document has no operations).

## Acceptance Criteria
- `sbt scalafmtCheck Test/scalafmtCheck test` green; `scripts/verify-asyncapi-v3.sh` green
- CI green on push

## Out of Scope
- Channel registration changes; event payload changes
