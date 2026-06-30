-- Flyway migration V1: initial schema for Order service

CREATE TABLE IF NOT EXISTS orders (
    id              UUID            NOT NULL DEFAULT gen_random_uuid(),
    order_description VARCHAR(255) NOT NULL,
    delivery_adress VARCHAR(255) NOT NULL,
    is_active BOOLEAN NOT NULL,
    version         BIGINT          NOT NULL DEFAULT 0,
    created_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_orders PRIMARY KEY (id)
);
