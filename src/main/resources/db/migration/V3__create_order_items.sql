-- Nested/collection fields aren't codegen-able, so order line items sit
-- outside the codegen'd "order" entity entirely. No FK on order_id yet
-- (that's a separate migration) - sku/product_name/unit_price_cents are
-- snapshotted at order-create time, not a live reference to catalog-service.
CREATE TABLE order_items (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    sku TEXT NOT NULL,
    product_name TEXT NOT NULL,
    unit_price_cents INT NOT NULL,
    quantity INT NOT NULL
);
