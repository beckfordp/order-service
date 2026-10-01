ALTER TABLE "order"
    ADD CONSTRAINT order_status_check
    CHECK (status IN ('pending', 'reserved', 'reservation_failed'));
