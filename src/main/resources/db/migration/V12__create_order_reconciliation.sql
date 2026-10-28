-- US-19.3: a capture failure flags the order for manual reconciliation instead
-- of automatically releasing its inventory reservations (the goods may already
-- be physically shipped). Store-level flag; no operator HTTP surface yet.
CREATE TABLE order_reconciliation (
    order_id UUID PRIMARY KEY REFERENCES "order" (id) ON DELETE CASCADE,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
