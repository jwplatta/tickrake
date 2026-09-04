# Intraday Index Write Lock Broken Across Docker Containers

## Problem

`Tickrake::Index::WriteLock` uses `flock(2)` to coordinate writes to the intraday index JSON
files (e.g. `SPXW.json`). On Linux, `flock` is process-scoped and works correctly within a
single host. On macOS Docker Desktop, however, `flock` calls between separate containers do
**not** see each other's locks — the Docker Desktop VM layer interposes on the syscall, so
each container effectively runs in isolation with respect to file locks.

This means when multiple option job containers write to the same `ROOT.json` file concurrently,
the write lock provides no protection and last-writer-wins races can produce a partially-written
or stale JSON file.

## Current State

The race condition exists but has not caused visible corruption in practice because:
1. Individual JSON writes are fast (sub-millisecond).
2. Each container writes a different root or writes infrequently enough that collisions are rare.

However, it is a real and latent bug that will surface under load or with more containers.

## Proposed Fix

Replace the append-to-JSON-file approach with an **append-only SQLite table**:

```sql
CREATE TABLE intraday_snapshots (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  collection_id TEXT NOT NULL,
  root          TEXT NOT NULL,
  provider      TEXT NOT NULL,
  expiration_date TEXT NOT NULL,
  file_path     TEXT NOT NULL,
  row_count     INTEGER,
  sampled_at    TEXT NOT NULL,
  created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
```

All containers `INSERT` rows atomically via SQLite WAL. No cross-container coordination
is needed because SQLite WAL handles concurrent writers natively.

To publish the intraday index JSON, query:

```sql
SELECT DISTINCT ON (expiration_date)
  expiration_date, file_path, row_count, sampled_at
FROM intraday_snapshots
WHERE root = ? AND DATE(sampled_at) = DATE('now')
ORDER BY expiration_date, sampled_at DESC;
```

This eliminates the `WriteLock` entirely for the intraday publisher and makes the index
derivable from durable state rather than an in-memory file that can be corrupted.

## Files Affected

- `lib/tickrake/index/write_lock.rb` — can be removed or made a no-op
- `lib/tickrake/index/intraday_publisher.rb` — replace file-write logic with SQLite insert + read-then-write
- `lib/tickrake/db/migrations/` — add migration to create `intraday_snapshots` table
