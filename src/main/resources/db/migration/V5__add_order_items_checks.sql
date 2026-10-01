-- Defense-in-depth, matching the order.status pattern (Scala-side validation
-- plus a DB-level CHECK): checkout's HTTP layer rejects non-positive
-- quantity/unit_price_cents before ever reaching the store, but these
-- constraints make that invariant hold even if something writes to this
-- table directly.
ALTER TABLE order_items
    ADD CONSTRAINT order_items_quantity_check CHECK (quantity > 0),
    ADD CONSTRAINT order_items_unit_price_cents_check CHECK (unit_price_cents >= 0);
