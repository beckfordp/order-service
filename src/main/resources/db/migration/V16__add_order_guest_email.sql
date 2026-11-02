-- ADM-10: anonymous checkouts have no user-service account, so the order
-- carries the guest's email for notification-service's recipient fallback
-- (ADR 0025 lookup at consumption). NULL for signed-in customers.
ALTER TABLE "order" ADD COLUMN guest_email text;
