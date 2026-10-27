-- Checkout Journey 1 (US-11.1, ADR 0018): orders are written once payment is
-- authorized, carrying a shipping address. Nullable: orders created before
-- US-11 have none. Old statuses stay allowed so existing rows still load.
ALTER TABLE "order" ADD COLUMN shipping_address JSONB;

ALTER TABLE "order"
    DROP CONSTRAINT order_status_check;

ALTER TABLE "order"
    ADD CONSTRAINT order_status_check
    CHECK (status IN ('pending', 'reserved', 'reservation_failed', 'confirmed', 'payment_failed', 'cancelled', 'authorized'));
