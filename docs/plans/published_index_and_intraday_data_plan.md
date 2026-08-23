# Published Index and Intraday Data Plan

## Goal

Make Tickrake the producer of a stable consumer contract for market data.

Consumers such as Options Monitor, QStudy, the research repo, and QuantRB should not read Tickrake's private SQLite DB or infer availability from Tickrake's working filesystem layout. They should read Tickrake-published index/latest files, then query the referenced data artifacts.

## Current Behavior To Preserve Initially

- Tickrake option jobs collect raw option-chain CSV snapshots throughout the day.
- Current raw option path shape:
  - `~/.tickrake/data/options/<provider>/<YYYY>/<MM>/<DD>/<ROOT>_exp<EXPIRATION>_<YYYY-MM-DD>_<HH-MM-SS>.csv`
  - Example: `~/.tickrake/data/options/schwab/2026/08/21/SPXW_exp2026-08-21_2026-08-21_13-30-15.csv`
- One provider/date directory contains many roots: `SPXW`, `SPY`, `QQQ`, stock roots, ETF roots, etc.
- Compaction currently runs by provider + option root + sample date.
- Current compacted path shape:
  - `~/.tickrake/data/options/<provider>/<YYYY>/<MM>/<DD>/<ROOT>_samples_<YYYY-MM-DD>.csv`
  - `~/.tickrake/data/options/<provider>/<YYYY>/<MM>/<DD>/<ROOT>_samples_<YYYY-MM-DD>.parquet`
- Current S3 archive mirrors the local `data_dir`-relative path:
  - `s3://tickrake/data/options/schwab/2026/08/21/SPXW_samples_2026-08-21.parquet`
- Current cleanup deletes raw sample CSVs after validated compaction/archive, deletes or retains compacted CSV depending on policy, and usually keeps compacted Parquet local.
- `tickrake.sqlite3` already records useful internal metadata:
  - `fetch_runs`: per fetch attempt, including job type, symbol, option root, resolved expiration, status, output path, timestamps.
  - `file_metadata_cache`: raw options, compacted CSV, compacted Parquet, provider, ticker/root, expiration date, row count, observed time range, storage format/location, artifact status, remote URI, source file count.

## Design Principles

- Tickrake SQLite remains internal.
- Published index/latest files become the external consumer contract.
- S3 remains the durable historical data plane.
- Intraday data uses a short-retention intraday object/cache service.
- Start with a shared Docker volume for intraday publication.
- Later replace or supplement that shared volume with MinIO if S3-compatible intraday object storage is useful.
- DuckDB can query both CSV and Parquet; the manifest should identify file format and URI.
- Redis is optional later for notification/state, not the authoritative data store.
- Consumers should cache reads/results for performance, but should not own a separate authoritative path catalog.
- Generate per-root JSON in Ruby from simple `file_metadata_cache` queries. Do not force all manifest assembly into one large SQL statement.

## Consumer Contract

Published files should answer two questions:

1. What historical datasets are available?
2. What intraday sample is the latest usable one?

Consumers should:

1. Read an index/latest JSON file.
2. Filter by provider/root/date/expiry/sample time.
3. Query the referenced files using DuckDB or pandas.
4. Treat the referenced URIs as consumer-readable locations.

Consumers should not:
- Read `tickrake.sqlite3` directly.
- List S3 directories to infer dataset completeness.
- Read Tickrake's private container paths.
- Depend on raw local paths such as `/home/tickrake/.tickrake/data/...`.

## Historical Publishing

Historical data means compacted, validated, archived artifacts.

Historical artifact unit:

- `dataset_type`: `options_compacted`
- key: `provider + root + sample_date`
- files: compacted Parquet required; compacted CSV optional

Existing S3 data path should remain stable:

```text
s3://tickrake/data/options/<provider>/<YYYY>/<MM>/<DD>/<ROOT>_samples_<YYYY-MM-DD>.parquet
s3://tickrake/data/options/<provider>/<YYYY>/<MM>/<DD>/<ROOT>_samples_<YYYY-MM-DD>.csv
```

Add published index files in the same S3 tree. Proposed minimal layout:

```text
s3://tickrake/data/options/<provider>/tickers.json
s3://tickrake/data/options/<provider>/<ROOT>.json
```

`tickers.json` is the provider-level discovery file. `<ROOT>.json` is the per-root index and includes enough historical detail for clients to query data files directly.

Minimal historical section inside `<ROOT>.json`:

```json
{
  "schema_version": 1,
  "provider": "schwab",
  "root": "SPXW",
  "updated_at": "2026-08-22T00:15:00Z",
  "historical": [
    {
      "sample_date": "2026-08-21",
      "status": "ready",
      "files": {
        "parquet": {
          "uri": "s3://tickrake/data/options/schwab/2026/08/21/SPXW_samples_2026-08-21.parquet",
          "row_count": 123456,
          "source_file_count": 65
        },
        "csv": {
          "uri": "s3://tickrake/data/options/schwab/2026/08/21/SPXW_samples_2026-08-21.csv",
          "row_count": 123456,
          "source_file_count": 65
        }
      },
      "first_observed_at": "2026-08-21T13:30:15Z",
      "last_observed_at": "2026-08-21T20:05:00Z"
    }
  ]
}
```

Historical publish sequence (a lot of this process should already exist except for steps 6-7)

1. Run existing compaction for provider/root/sample_date.
2. Validate compacted output against raw source files.
3. Upload compacted artifacts to S3.
4. Verify uploaded objects.
5. Update `file_metadata_cache`.
6. Update provider-level `tickers.json` if this root is new.
7. Update per-root `<ROOT>.json` historical section.
8. Only then allow cleanup of raw intraday source files according to existing retention policy.

Historical index source query:

```sql
SELECT
  provider_name,
  ticker AS root,
  substr(path, instr(path, '_samples_') + 9, 10) AS sample_date,
  storage_format,
  remote_uri,
  path,
  row_count,
  source_file_count,
  first_observed_at,
  last_observed_at,
  artifact_status,
  storage_location,
  updated_at
FROM file_metadata_cache
WHERE dataset_type IN ('options_compacted_csv', 'options_compacted_parquet')
  AND provider_name = ?
  AND ticker = ?
ORDER BY sample_date, storage_format;
```

Build the `historical` array in Ruby by grouping these rows by `sample_date`. Prefer `remote_uri` for compacted artifacts once archive verification has succeeded.

## Intraday Publishing

Intraday data means current-day raw/latest samples before final compaction.

Initial implementation should use raw CSV snapshots, not intraday Parquet.

Intraday raw artifact unit:

- `dataset_type`: `options_intraday_snapshot`
- key: `provider + root + collection_id`
- `collection_id` identifies one logical run of the option sampling routine.
- Today, Tickrake effectively has this because one `run_time` is passed through the whole options queue and stored as `fetch_runs.scheduled_for` / `file_metadata_cache.last_observed_at`.
- Add an explicit `collection_id` to the internal metadata so grouping does not depend on timestamp parsing forever.
- files: one CSV per expiration fetched for that root/collection

Required schema additions:

```sql
ALTER TABLE fetch_runs ADD COLUMN collection_id TEXT;
ALTER TABLE file_metadata_cache ADD COLUMN collection_id TEXT;

CREATE INDEX idx_fetch_runs_collection
ON fetch_runs (dataset_type, option_root, collection_id);

CREATE INDEX idx_file_metadata_collection
ON file_metadata_cache (dataset_type, provider_name, ticker, collection_id);
```

Recommended collection ID format:

```text
options-<provider>-<root>-<YYYYMMDDTHHMMSSZ>
```

Example:

```text
options-schwab-SPXW-20260823T154210Z
```

Assign `collection_id` once at the start of `OptionsJob#run`, then pass it through the queue the same way `run_time` is passed today.

Use an intraday shared volume first.

Example Compose mount:

```yaml
services:
  tickrake:
    volumes:
      - tickrake-intraday:/intraday

  options-monitor:
    volumes:
      - tickrake-intraday:/intraday:ro
```

Tickrake should publish consumer-readable paths derived from the configured storage paths. Do not hardcode `/intraday/tickrake/data`; that is only one likely container configuration.

For the first vertical slice, point Tickrake's existing storage config directly at the shared volume:

```yaml
storage:
  data_dir: /intraday/tickrake/data
```

Tickrake already derives `options_dir` as `<data_dir>/options`, so this example makes raw option samples land at:

```text
/intraday/tickrake/data/options/<provider>/<YYYY>/<MM>/<DD>/*.csv
```

This is simpler than writing to a private container path and replicating into the shared volume. It preserves the existing path model and avoids a second publish/copy failure mode. Options Monitor may see files in the shared volume before they are indexed, but it should ignore unindexed files; `<ROOT>.json` is the consumer contract.

In local non-container development, the same logic may produce URIs such as:

```text
file:///Users/jplatta/.tickrake/data/options/schwab/2026/08/23/...
```

The rule is: intraday file URI = `file://` plus the actual path produced from Tickrake config.

Defer private staging plus replication until there is a concrete need, such as publish validation before visibility, separate intraday retention, or a MinIO-backed intraday store.

Intraday shared-volume layout:

```text
/intraday/tickrake/data/options/<provider>/<YYYY>/<MM>/<DD>/
  <ROOT>_exp<EXPIRATION>_<YYYY-MM-DD>_<HH-MM-SS>.csv

/intraday/tickrake/data/options/<provider>/<ROOT>.json
/intraday/tickrake/data/options/<provider>/tickers.json
```

The date/provider folder may contain many roots, matching current Tickrake behavior.

Minimal per-root index with historical and intraday sections:

```json
{
  "schema_version": 1,
  "provider": "schwab",
  "root": "SPXW",
  "updated_at": "2026-08-23T20:15:00Z",
  "historical": [
    {
      "sample_date": "2026-08-21",
      "status": "ready",
      "files": {
        "parquet": {
          "uri": "s3://tickrake/data/options/schwab/2026/08/21/SPXW_samples_2026-08-21.parquet",
          "row_count": 4332126,
          "source_file_count": 9609
        },
        "csv": {
          "uri": "s3://tickrake/data/options/schwab/2026/08/21/SPXW_samples_2026-08-21.csv",
          "row_count": 4332126,
          "source_file_count": 9609
        }
      },
      "first_observed_at": "2026-08-21T13:32:26Z",
      "last_observed_at": "2026-08-21T20:04:52Z"
    }
  ],
  "intraday": {
    "collection_id": "options-schwab-SPXW-20260823T154210Z",
    "sample_date": "2026-08-23",
    "sampled_at": "2026-08-23T15:42:10Z",
    "status": "complete",
    "expected_expiration_count": 31,
    "received_expiration_count": 31,
    "files": [
      {
        "expiration_date": "2026-08-23",
        "format": "csv",
        "uri": "file:///intraday/tickrake/data/options/schwab/2026/08/23/SPXW_exp2026-08-23_2026-08-23_15-42-10.csv",
        "row_count": 510
      },
      {
        "expiration_date": "2026-08-24",
        "format": "csv",
        "uri": "file:///intraday/tickrake/data/options/schwab/2026/08/23/SPXW_exp2026-08-24_2026-08-23_15-42-10.csv",
        "row_count": 620
      }
    ]
  }
}
```

Status values:

- `complete`: all expected expirations for the job/root/sample are present.
- `partial`: some expected expirations failed or are missing.
- `failed`: no usable snapshot should be consumed.

Initial policy:

- Only advance the `intraday` section of the per-root JSON for `complete`.
- Keep `partial` collection records in internal metadata for diagnostics.
- Degraded/partial latest can be added later if Options Monitor explicitly handles it.

Intraday publish sequence:

1. Option job fetches raw CSV snapshots as it does today.
2. Tickrake assigns one `collection_id` for the logical sampling run and records it with `fetch_runs` and raw `options` rows in `file_metadata_cache`.
3. After one scheduled job iteration finishes, group successful raw files by provider/root/collection_id.
4. Evaluate expected vs received expirations for the scheduled job/root.
5. If complete, write/update the per-root JSON file, e.g. `<options_dir>/<provider>/<ROOT>.json`.
6. Advance the `intraday` section to the new collection last.
7. If partial, do not advance `intraday`; log/store the partial collection internally for debugging.

Do not update the per-root JSON before all referenced files are present and readable from the consumer-visible path.

Intraday source query:

```sql
WITH latest AS (
  SELECT collection_id
  FROM file_metadata_cache
  WHERE dataset_type = 'options'
    AND provider_name = ?
    AND ticker = ?
    AND collection_id IS NOT NULL
  GROUP BY collection_id
  ORDER BY MAX(last_observed_at) DESC
  LIMIT 1
)
SELECT
  provider_name,
  ticker AS root,
  collection_id,
  date(last_observed_at) AS sample_date,
  last_observed_at AS sampled_at,
  expiration_date,
  path,
  row_count,
  file_size,
  updated_at
FROM file_metadata_cache
WHERE dataset_type = 'options'
  AND provider_name = ?
  AND ticker = ?
  AND collection_id = (SELECT collection_id FROM latest)
ORDER BY expiration_date;
```

Build the `intraday` object in Ruby from this result. Convert each `path` to a file URI from the actual configured storage path. If no rows exist, emit `"intraday": null` or omit the key. Prefer `"intraday": null` so consumers can distinguish no live data from a schema problem.

Current cleanup behavior is correct: after compaction, validation, archive, and source cleanup, raw option CSV files are deleted and their `file_metadata_cache` rows are deleted. Intraday entries do not need to survive cleanup. Rebuilding `<ROOT>.json` after cleanup should naturally remove or null out the `intraday` section while leaving the `historical` section intact.

Raw sample retention policy:

- Delete raw samples after successful compaction, validation, archive upload, and archive verification.
- Keep raw samples only as short-lived intraday/staging material.
- Target retention for raw samples that have not yet been cleaned: 2-3 days.
- Never delete raw samples solely because they are old if their compacted artifacts are not verified in S3.
- Add a cleanup pass for raw samples older than the retention window once their compacted CSV/Parquet artifacts are verified remote.

## Index Write Locking

Multiple jobs can update the same per-root JSON. Example SPXW writers:

- `spx_0dte_options`
- `spx_short_dated_options`
- `spx_longer_dated_options`
- `compact_spxw`
- archive/cleanup steps

Protect each provider/root index with a mutex keyed by:

```text
index:options:<provider>:<root>
```

For the first local/container implementation, use a filesystem lock:

```text
~/.tickrake/locks/index-options-<provider>-<root>.lock
```

The write sequence must run fully under the lock:

1. Acquire exclusive lock.
2. Query compacted historical metadata.
3. Query latest raw intraday metadata.
4. Build complete `<ROOT>.json` in memory.
5. Write temp file.
6. Fsync temp file.
7. Atomically rename temp file to target.
8. Release lock.

Do not query/generate outside the lock and write later; another job could update the same root in between.

Use the same locked writer after source cleanup. At that point raw rows should be gone from `file_metadata_cache`, so the regenerated per-root JSON should have `"intraday": null` while preserving verified historical entries.

For historical S3 publishing, a local lock is enough only while there is one Tickrake writer instance. If multiple hosts write the same S3 index later, use a distributed lock or enforce a single writer.

## MinIO Later

MinIO is the likely next step if the shared volume becomes too limiting.

With MinIO, keep the same conceptual layout but change URIs:

```text
s3://tickrake-intraday/tickrake/data/options/schwab/2026/08/21/SPXW_exp2026-08-21_2026-08-21_20-04-52.csv
s3://tickrake-intraday/tickrake/data/options/schwab/SPXW.json
```

The consumer contract stays the same:

- Read latest/index.
- Query listed files.
- Do not know Tickrake internals.

## Options Monitor Changes

Current Options Monitor behavior:

- Uses Tickrake SQLite `file_metadata_cache` to discover raw CSV snapshots.
- Loads current snapshots from local CSV paths with pandas.
- Uses DuckDB over local compacted daily Parquet for historical views.
- Builds date/symbol/expiry lists from SQLite queries.

Target behavior:

- Replace SQLite discovery with published index/latest discovery.
- Keep similar functions:
  - `list_expirations`
  - `list_snapshot_dates`
  - `find_latest_snapshots`
  - `find_snapshots_for_expiry_on_date`
  - historical DuckDB loaders
- Back those functions with manifests instead of direct SQLite.
- Intraday:
  - read `/intraday/tickrake/data/options/<provider>/<ROOT>.json`
  - query listed CSV files by URI/path
- Historical:
  - read S3 index/catalog
  - query listed S3 Parquet files directly with DuckDB `httpfs`, or use a read-through local cache.

Options Monitor cache policy:

- No authoritative persistent path database.
- Cache manifest reads briefly.
- Cache intraday latest for seconds.
- Cache historical indexes for minutes.
- Cache historical query results longer because compacted S3 Parquet is immutable.
- Optional read-through file cache for S3 Parquet is allowed for performance.

## DuckDB Notes

- DuckDB can read CSV and Parquet.
- DuckDB can read S3 Parquet using `httpfs` with S3 credentials.
- Direct S3 reads are acceptable for first implementation.
- A local read-through cache can be added if dashboard queries repeatedly hit the same historical files.
- Avoid trying to append directly to a single Parquet file in S3; S3 object updates are whole-object overwrites.

## S3 Update Notes

- Updating small JSON index/latest objects regularly is acceptable.
- Historical S3 data should remain durable and verified.
- Intraday S3/MinIO data should be short-retention and can be treated as cache.
- Do not rely on directory listing order for freshness.
- The per-root JSON file is the mutable pointer; data artifacts should be immutable or treated as replaceable cache depending on tier.

## Tickrake Implementation Plan

### Phase 1: Published Index Types

- Add index/manifest builders that consume `file_metadata_cache` rows from the two simple queries above.
- Add URI abstraction:
  - local consumer path, e.g. `file:///intraday/tickrake/data/options/...`
  - S3 URI, e.g. `s3://tickrake/data/options/...`
  - future MinIO URI, e.g. `s3://tickrake-intraday/...`
- Add JSON schema version field.
- Add explicit `collection_id` to `fetch_runs` and `file_metadata_cache`.
- Add per-provider/per-root index write lock.
- Add atomic JSON writer: temp file, fsync, rename under the per-root lock.
- Add tests for JSON shape and path/URI generation.

### Phase 2: Historical Index Publisher

- Hook into successful archive step for compacted option samples.
- Update provider-level `tickers.json` when new roots appear.
- Update per-root `<ROOT>.json` historical section.
- Regenerate per-root `<ROOT>.json` after source cleanup so `intraday` is removed/null when raw rows are deleted.
- Ensure published index is based on verified remote URI and metadata cache, not file listing.
- Add specs around archive success, partial archive, missing Parquet, and index update ordering.

### Phase 3: Intraday Shared-Volume Publisher

- Add config for intraday publish root, e.g. `/intraday`.
- After scheduled options job iteration, group raw `options` metadata rows by provider/root/collection_id.
- Update per-root `<ROOT>.json` intraday section.
- Advance `intraday` only after the full collection iteration completes and all expected expirations succeeded.
- Do not advance `intraday` for partial collections; store/log partial state internally.
- Start with SPXW only if needed.
- Add specs for complete sample, partial sample, and latest update behavior.

### Phase 4: Options Monitor Migration

- Add manifest-backed data source in Options Monitor.
- Keep existing local SQLite-backed source temporarily as compatibility mode.
- Switch dashboard reads to manifest-backed discovery.
- Use DuckDB/pandas against manifest-listed URIs.

### Phase 5: MinIO Intraday Backend

- Add MinIO service in `quant-infra`.
- Add Tickrake intraday object backend that writes to MinIO.
- Change intraday manifest URIs from `file:///intraday/tickrake/data/options/...` to `s3://tickrake-intraday/tickrake/data/options/...`.
- Keep Options Monitor consumer logic mostly unchanged.

## Deferred

- Moving Tickrake metadata from SQLite to Postgres.
- Making Options Monitor read Tickrake SQLite in production.
- Redis as authoritative intraday file store.
- Redis event bus.
- Degraded latest snapshots.
- Continuously rewritten intraday Parquet.
- Iceberg/Delta/Hudi table formats.
- General serving API.
- Kubernetes or extra orchestration.

## First Vertical Slice

Target the smallest useful slice:

1. SPXW options only.
2. Shared Docker volume intraday publication.
3. Local per-root JSON file under `/intraday/tickrake/data/options/schwab/SPXW.json`.
4. Manifest lists raw CSVs under `/intraday/tickrake/data/options/schwab/YYYY/MM/DD/`.
5. Options Monitor reads this manifest and loads the listed CSVs.
6. Historical SPXW compacted Parquet manifest published to S3 after archive.

This proves the contract before changing the whole storage model.
