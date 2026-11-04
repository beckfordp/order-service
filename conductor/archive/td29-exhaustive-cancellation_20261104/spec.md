# Spec: Exhaustive `OrderCancellation` match (gluon TD-29)

## Overview
`OrderCancellation.reasonFor` / `actorFor` end in `case _ => None` over the
sealed `OrderEvent`, against `scala-fp.md` ("pattern matches on sealed types are
exhaustive with no `case _`"). A new cancelling event would compile and silently
write no cancellation audit row.

## Functional Requirements
1. Both matches list every `OrderEvent` case: `PaymentCaptured`,
   `PaymentFailed(_)`, `DispatchConfirmed`, `DispatchFailed` map to `None`.
2. No behaviour change.

## Acceptance Criteria
- No `case _` in `OrderCancellation.scala`.
- `OrderCancellationSuite` covers every `OrderEvent`: reason and actor are both
  `Some` (cancelling) or both `None`.
- `sbt scalafmtCheck test` green.

## Out of Scope
- Making non-exhaustive matches a compile error (`-Wconf`/`-Werror`) — today
  they only warn; separate decision.
