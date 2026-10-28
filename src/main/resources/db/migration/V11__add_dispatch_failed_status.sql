-- US-18.3: a fulfilment DispatchFailed moves an authorized order to dispatch_failed.
ALTER TABLE "order"
    DROP CONSTRAINT order_status_check;

ALTER TABLE "order"
    ADD CONSTRAINT order_status_check
    CHECK (status IN ('pending', 'reserved', 'reservation_failed', 'confirmed', 'payment_failed', 'cancelled', 'authorized', 'dispatched', 'dispatch_failed'));
