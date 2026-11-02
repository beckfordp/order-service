-- ADM-4/ADR 0026: the reconciliation queue's open/resolved workflow.
-- Existing first-wins flags become `open`; the recording path is unchanged.
ALTER TABLE order_reconciliation
    ADD COLUMN status TEXT NOT NULL DEFAULT 'open',
    ADD COLUMN operator_id TEXT,
    ADD COLUMN note TEXT,
    ADD COLUMN resolved_at TIMESTAMPTZ;

ALTER TABLE order_reconciliation
    ADD CONSTRAINT order_reconciliation_status_check
        CHECK (status IN ('open', 'resolved'));

-- A resolved flag must carry its resolver; an open one must not.
ALTER TABLE order_reconciliation
    ADD CONSTRAINT order_reconciliation_resolution_check
        CHECK (
            (status = 'open'
                AND operator_id IS NULL
                AND resolved_at IS NULL)
            OR (status = 'resolved'
                AND operator_id IS NOT NULL
                AND resolved_at IS NOT NULL)
        );

CREATE INDEX order_reconciliation_status_created_at_idx
    ON order_reconciliation (status, created_at DESC);
