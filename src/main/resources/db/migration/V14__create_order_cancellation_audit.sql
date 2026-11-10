-- SPDX-License-Identifier: MIT
-- US-22: who cancelled an order, persisted with the cancellation transition.
-- One row per cancelled order; written in the same transaction as the status
-- update and OrderCancelled outbox row.
create table order_cancellation_audit (
    order_id uuid primary key
        references "order" (id) on delete cascade,
    actor_type text not null
        check (actor_type in ('customer', 'operator', 'system')),
    actor_id text not null,
    reason text not null
        check (reason in ('customer', 'auth_expired', 'stale', 'operator')),
    created_at timestamptz not null default now()
);
