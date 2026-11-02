# Specification: TD-21 — retire legacy order statuses

## Overview

`pending`, `reserved` and `reservation_failed` predate ADR 0018; nothing
writes them and no order row exists before authorization. Remove them from
the domain, the DB constraint and live references.

User decision: existing legacy rows are test data only — delete them.

## Functional Requirements

- `OrderStatus` drops the three cases and `fromString` arms; only
  `authorized/confirmed/payment_failed/cancelled/dispatched/dispatch_failed`
  remain.
- `V13__retire_legacy_order_statuses.sql` deletes legacy rows (cascading
  `order_items`) and narrows `order_status_check` to the six live values.
- `OrderHistoryCache` keys are versioned (`order-history:v2:`) so cached JSON
  containing retired statuses/deleted orders is ignored.
- Tests no longer seed legacy states; a V12→V13 migration test proves the
  delete/narrow path. Scripts/README/product docs use live states.
- gshop's `OrderStatus` union and fixtures drop the three values.

## Non-Functional Requirements

- `sbt scalafmtAll scalafmtCheckAll test` green; `schemaRegistryCheck` green.
- Migration is destructive but local-only; no old image supported after it.

## Acceptance Criteria

- New orders/transitions only use the six live states.
- Legacy rows are gone after migration and the CHECK rejects the retired
  values.
- No live code/doc/test references remain; historical records may.

## Out of Scope

Notification's broader historical docs beyond live contract sections;
payment `pending`; fulfilment `PENDING`; production migration/rollback.
