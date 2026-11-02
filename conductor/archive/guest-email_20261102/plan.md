# Plan: ADM-10 — guest checkout email

## Phase 1: Domain and persistence

- [x] Task: Failing `GuestEmailSuite`, `MigrationsSuite` column and
  `CreateAuthorizedContract` round-trip (get/listByCustomer/listAll; signed-in
  None); add `GuestEmail`, V16, `Order.guestEmail` and the store columns.

## Phase 2: Routes

- [x] Task: Failing route tests (normalized email stored and returned on
  POST/GET; malformed → 400 with no reserve); wire request/response.
- [x] Task: Conductor - User Manual Verification 'Guest email'
  (Protocol in workflow.md) — prompting off: full suite green (252 passed).

## Review

- Reviewed 2026-11-02 (conductor:review): plan compliance yes; style pass;
  validation precedes the reserve; old cached history entries decode with
  `guestEmail` absent. No blocking findings.
