-- Table name is quoted throughout as a defensive habit: several plausible domain
-- names (e.g. "user", "group", "order") are reserved PostgreSQL keywords.
CREATE TABLE "order" (
    id UUID PRIMARY KEY,
    customer_id TEXT NOT NULL,
    total_cents INT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
