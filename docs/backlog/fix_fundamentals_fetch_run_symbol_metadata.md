---
type: bug
tags: [fundamentals, metadata-sync, postgres, sidecar]
title: Make fundamentals fetch-run metadata ingestible by PostgreSQL
description: Align fundamentals sidecar symbol metadata with the fetch_runs schema and recover pending null-symbol sidecars.
created: 2026-10-15
updated: 2026-10-29
status: complete
priority: high
source: tickrake
---

# Summary

The fundamentals job writes successful run sidecars with `fetch_run.symbol: null`, but PostgreSQL defines `fetch_runs.symbol` as `NOT NULL`. `MetadataSyncJob` therefore fails its database transaction on fundamentals sidecars, leaves them in `pending_metadata`, and retries the same failure. Define a clear representation for a run that covers multiple configured symbols and keep current and future sidecars compatible with both SQLite and PostgreSQL.

## Requirements

- Choose and document one representation for a fundamentals run's run-level symbol: either the literal `none` sentinel or nullable `symbol` support in both database backends. Do not use a claim that the run covered the entire market; the collection universe is the configured fundamentals symbol list.
- Ensure new fundamentals sidecars use the chosen representation and `MetadataSyncJob` can ingest them without violating database constraints.
- Make existing pending fundamentals sidecars with `symbol: null` ingestible, either by safely normalizing them during ingestion or by an explicit data/schema migration. Preserve their run status, timestamps, collection IDs, output paths, and file metadata.
- Keep per-symbol values in the fundamentals Parquet data unchanged; this field describes the run-level fetch record only.
- Add tests for successful fundamentals sidecar ingestion on SQLite and PostgreSQL, including compatibility with already-pending null-symbol sidecars and preservation of `file_metadata_cache` upsert behavior.
- Verify metadata sync drains the affected sidecars and stops retrying the PostgreSQL `NOT NULL` violation.

## Dependencies & Resources

- `lib/tickrake/fundamentals_job.rb`: writes the fundamentals `fetch_run` sidecar with `symbol: null` and file metadata with `ticker: "all"`.
- `lib/tickrake/metadata_sync_job.rb`: ingests pending sidecars transactionally and retains them when ingestion fails.
- `lib/tickrake/tracker.rb`: inserts fetch runs and upserts file metadata.
- `lib/tickrake/db/postgres_migrations/001_create_schema.rb` and `lib/tickrake/db/migrations/001_create_fetch_runs.rb`: define `fetch_runs.symbol` as non-null.
