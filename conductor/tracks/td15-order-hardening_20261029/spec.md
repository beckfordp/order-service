# TD-15.1 — Cross-repo consistency hardening (order-service)

## Overview

Order-service slice of gluon TD-15 (`gluon/TECHNICAL_DEBT.md`). Brings the
order event contract in line with the per-outcome style used by
payment/fulfilment services, and fixes boundary/purity patterns the
full-codebase review found: raw String ids, unsafe URI parsing, `sys.env`
config, silent unknown-event handling, raw payload logging, missing property
tests, over-broad `Async` constraints.

## Functional Requirements

1. **Order-event split** — replace `OrderStatusChanged` with `OrderConfirmed`,
   `OrderPaymentFailed {reason}`, `OrderCancelled`; emit the matching event
   from each consumer outbox write; register 3 `Publish(PublisherKind.Cdc)`
   AsyncAPI channels; retire the old producer, consumer mirror (other repo)
   and registry artifact.
2. **Typed `OrderId`** — opaque UUID in `domain/`, parsed once at the
   HTTP/event boundary with `Either.catchOnly`; routes `path[OrderId]`; store
   takes `OrderId`; malformed id → `400` (never not-found/false); remove all
   `Try(UUID.fromString).toOption` and bare `UUID.fromString`.
3. **Config** — client `baseUrl` loaded as `Uri` (`ConfigReader` /
   `loadValidated`); `asyncApiHtmlDir` config field with
   `${?ASYNCAPI_HTML_DIR}` override; no `sys.env` in `AppRoutes`, no
   `Uri.unsafeFromString`.
4. **Observability** — unknown Kafka `eventType` logged with topic/type (not
   silently ignored); raw payloads never logged at error (log
   topic/eventType/byte length/error class).
5. **Tests** — add `munit-scalacheck`; properties for `OrderId` parse
   round-trip and item/total arithmetic.
6. **Effect constraints** — `Cors` → `Applicative`; `HealthRoutes` →
   `Applicative`/`Monad`; route endpoints → `Monad`/`MonadThrow`; clients →
   `Concurrent`/`Clock`; keep `Async` only where skunk/Kafka/fs2 require it.

## Non-Functional Requirements

- Behaviour-preserving apart from the retired event type and the stricter
  malformed-id response.
- `sbt scalafmtCheckAll scalafmtSbtCheck test` green; coverage >80%.
- Live contract matches `gluon/docs/system-design.md`; `schemaRegistryCheck`
  green; old artifact removed from the local registry.

## Acceptance Criteria

- No `OrderStatusChanged` references remain in repo code, tests, scripts, or
  AsyncAPI docs.
- Malformed order id yields `400`; every id is parsed once at the edge.
- No `sys.env` or `unsafeFromString` in main code.
- Unknown event types logged; raw payloads absent from logs; tests assert both.
- Full suite, scalafmt, coverage, and schema-registry checks pass.

## Out of Scope

- Lifecycle state machine, PATCH/PUT transition guard, store `Either`
  semantics, 404-vs-409 distinction, N+1/transaction fixes, cache fallback,
  reconciliation flag (Phase 3 of the approved cross-repo plan).
- Other service repos (TD-15.2–15.8) and generator backport (Phase 4).
