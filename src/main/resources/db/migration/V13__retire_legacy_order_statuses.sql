-- SPDX-License-Identifier: MIT
-- TD-21: `pending`/`reserved`/`reservation_failed` predate ADR 0018 and are
-- never written now. There is no production data; delete the test-only legacy
-- rows (order_items cascade) rather than invent terminal outcomes for them.
DELETE FROM "order"
WHERE status IN ('pending', 'reserved', 'reservation_failed');

ALTER TABLE "order" DROP CONSTRAINT order_status_check;
ALTER TABLE "order"
    ADD CONSTRAINT order_status_check
    CHECK (status IN ('confirmed', 'payment_failed', 'cancelled',
                      'authorized', 'dispatched', 'dispatch_failed'));
