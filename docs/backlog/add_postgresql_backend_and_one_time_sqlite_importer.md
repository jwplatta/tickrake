---
type: feature
tags: [tickrake, postgres, sqlite-migration]
title: Add PostgreSQL backend and one-time SQLite state importer
description: Support PostgreSQL alongside default SQLite and provide an explicit importer for moving existing state once.
created: 2026-10-14
updated: 2026-10-14
status: complete
priority: critical
source: tickrake
---

# Summary

Add PostgreSQL as an explicitly selectable Tickrake metadata and shared
rate-limit backend while retaining SQLite as the default and fully supported
backend. Provide an operator-run utility to copy the existing SQLite state to
PostgreSQL once. Normal Tickrake startup must not check for or attempt to
migrate data between backends.

## Requirements

- Add a backend configuration that selects SQLite by default and supports an
  explicitly configured PostgreSQL connection. Keep existing SQLite users,
  paths, and behavior compatible.
- Route the tracker, data loader, explicit schema migration command, index
  data, fetch history, file metadata, and shared rate limiter through the
  selected backend. Add backend-specific migrations for the current logical
  schema, including primary keys, constraints, indexes, and rate-limit state.
- Add the PostgreSQL driver and required client libraries to the Tickrake
  runtime/Docker image. Do not make PostgreSQL a requirement for SQLite users.
- Provide a separate one-time SQLite-to-PostgreSQL importer with dry-run
  support. It must require an empty target, copy all current tables and IDs,
  preserve rate-limit balances, and verify copied row counts and generated ID
  sequences. Fail clearly on a non-empty or incompatible target.
- Keep the importer out of job startup, CLI initialization, and normal
  migrations. Do not add automatic discovery, repeated import checks, or
  dual-write behavior.
- Add specs for both adapters, SQLite compatibility, importer dry-run and
  empty-target enforcement, data/count/ID preservation, and sequence
  verification. Update user documentation and the Unreleased changelog.

## Dependencies & Resources

- Quant-infra runtime wiring: `docs/backlog/move_tickrake_production_to_postgres.md`
  in the quant-infra repository
- `lib/tickrake/tracker.rb`, `lib/tickrake/data_loader.rb`,
