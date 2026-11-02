# Plan: TD-21 — retire legacy order statuses

## Phase 1: Domain + migration

- [x] Task: Failing tests — fromString rejects legacy, V12→V13 deletes rows, constraint rejects legacy (Red)
- [x] Task: Remove enum cases; add V13 migration; version cache keys (Green)
- [x] Task: Replace legacy seeds in lifecycle/store/consumer/migration/cache tests
- [x] Task: Conductor - User Manual Verification 'Domain + migration' (Protocol in workflow.md)

## Phase 2: References + verification

- [x] Task: Update scripts, README, product docs, build comment
- [x] Task: `sbt scalafmtAll scalafmtCheckAll test`; schema registry check
- [x] Task: Conductor - User Manual Verification 'References + verification' (Protocol in workflow.md)

## Review (2026-11-02)

- `sbt scalafmtAll scalafmtCheckAll test` green (200 tests), including a
  targeted V12→V13 delete/narrow migration test.
- `verify-updatestatusifcurrent.sh` PASS: V1–V13, live values accepted,
  retired values rejected, guarded update/no-op intact.
- Live cluster: migration V13 applied, zero legacy rows remain; the
  auth-expiry E2E and demo seed both pass afterwards.
- No blocking findings.
