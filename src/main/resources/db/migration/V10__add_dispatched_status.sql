-- US-17.4: fulfillment-service's DispatchConfirmed moves authorized orders
-- to dispatched.
ALTER TABLE "order"
    DROP CONSTRAINT order_status_check;

ALTER TABLE "order"
    ADD CONSTRAINT order_status_check
    CHECK (status IN ('pending', 'reserved', 'reservation_failed', 'confirmed', 'payment_failed', 'cancelled', 'authorized', 'dispatched'));
