# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed
- Updated `DuckdbOptionCompactedWriter` to sort option sample Parquet files by `expiration_date, sampled_at, strike, contract_type, symbol` to optimize Parquet row-group pruning for expiration filters and preserve chain snapshot locality for backtesting.
- Updated option sample Parquet compaction to use `ZSTD` compression (matching Apache Iceberg standards) and `ROW_GROUP_SIZE 50000`.

### Removed
- Removed support for defining scheduled jobs in YAML configuration (`schedule`). All jobs are now defined using the Ruby DSL (`Tickrake.job`). `ConfigLoader` now rejects YAML configuration containing a `schedule` mapping with an explicit error.

### Added
- Added `underlying_symbol` (e.g. `$SPX`) and `option_root` (e.g. `SPXW`) columns to option chain sample CSV files across live Schwab collection (`OptionsJob`) and historical Massive imports (`MassiveOptionsImporter`).
- Added date-partitioned daily candle storage (`data/candles/<provider>/<YYYY>/<MM>/<DD>/<frequency>/<symbol>.csv`) across streaming (`StreamJob`, `ChartStreamJob`) and batch collection (`CandlesJob`) to support idempotent downstream Iceberg table ingestion.
- Added structured chart stream write logs with bar-count and CSV-byte deltas for data-volume dashboard metrics.
- Added standalone migration script `scripts/rewrite_options_parquet.rb` to re-encode and re-sort existing option sample Parquet files in-place with atomic verification.
- Added per-service delivery counters, accepted/discarded record metrics, and last-event timestamps to `StreamJob` to prevent chart traffic from masking silent Level One feeds.
- Added `byte_count` to candle `fetch_success` logs for data-volume dashboard metrics.

### Fixed
- Fixed database connection errors (such as authentication or unreachable host failures) being erroneously reported as `"Database migrations are pending"`. Added `Tickrake::DatabaseConnectionError` and immediate connection validation on establishment.
- Fixed symbol normalization in `Paths#candle_path` to consistently apply the provider's `symbol_map` (e.g., `/ES` -> `^ES`).
- Fixed date extraction in `StagingPublisher` to support date-partitioned candle paths.
- Fixed missing `install_signal_handlers`, `mark_iteration_success`, `in_window?`, and `log_iteration_failure` methods in `StagingPublisherSchedulerRunner`.

- Removed dead AWS S3 cold archival pipeline, AWS Glacier storage classes, S3 manifest writers, and S3 manifest reconciler (`ReconcilerJob`, `ReconcilerRunner`, `DSL::ReconcileBuilder`, `Maintenance::Candles::Archiver`, `Maintenance::Candles::ManifestWriter`, `Maintenance::OptionSamples::ArtifactArchiver`, `Maintenance::OptionSamples::ManifestWriter`, `Maintenance::OptionSamples::LocalArtifactManager`).
- Removed obsolete S3 manifest guards from `SourceSampleCleaner`, keeping local raw CSV pruning safely managed by compaction validation.
- Removed dead S3 upload method and datastore option from `EventsIngestorJob` and `EventsIngestBuilder`.
- Removed `archive` task from `MaintenanceBuilder` and updated `ConfigLoader` to reject legacy `archive` tasks.
- Removed obsolete scripts `scripts/backfill_manifests.rb` and `scripts/rearchive_candles_to_s3.rb`.

### Added
- Added local file retention policy and immediate deletion management to `HistoricalPublisher` (`retain_days`, `delete_after_upload`), supporting automated local cleanup while strictly exempting `candles` files to prevent backfill regressions.
- Added migration `003_add_local_deleted_at_to_historical_file_ledger.rb` and `HistoricalFileLedger` scopes (`locally_retained`, `eligible_for_retention_cleanup`) and `mark_locally_deleted!` method to track pruned local files while maintaining the complete audit trail.
- Added `--delete-after-upload` and `--retain-days` flags to `tickrake publish-historical` CLI command and DSL `historical_publish` block, unifying `retain_days: 0` with immediate post-upload deletion.
- Added MinIO historical publishing pipeline (`HistoricalPublisher`, `HistoricalManifestWriter`, `HistoricalPublisherJob`, `HistoricalPublisherSchedulerRunner`, `historical_publish` DSL block): scans processed/compacted Parquet datasets (`options`, `candles`, `level_one`, `order_book`, `fundamentals`, `economic_events`) and mirrors the local directory hierarchy in MinIO historical storage.
- Added database ledger `historical_file_ledger` via ActiveRecord migration `002_create_historical_file_ledger.rb` and model `HistoricalFileLedger` with safe idempotent upserts, tracking uploaded files, row counts, file sizes, SHA256 checksums, and publish timestamps.
- Added unidirectional JSON audit manifests in MinIO (`manifests/<dataset>.json`) detailing file lists, totals, row counts, and checksums for external discovery.
- Added `tickrake publish-historical` CLI command supporting `--datastore`, `--dataset`, and `--batch-size` options for manual runs and backfilling.
- Decoupled staging directories (`pending_metadata_dir` and `pending_events_dir`) by defaulting them to `~/.tickrake/staging/` with automatic fallback to legacy paths in `~/.tickrake/data/` if unmigrated files exist.
- Added `minio` to `VALID_DATASTORE_TYPES` in `ConfigLoader`.
- Added standalone `ActiveRecord` (`>= 7.1`) to manage data persistence and migrations across SQLite and PostgreSQL backends.
- Added ActiveRecord domain models in `Tickrake::Models` (`FetchRun`, `FileMetadata`, `MarketIndex`, `Ticker`, `TickerAlias`, `MarketIndexMembership`, `ApiRateLimit`).
- Added single canonical ActiveRecord migration (`001_create_tickrake_schema.rb`) for unified, cross-database schema management.
- Added PostgreSQL metadata and shared rate-limit backend alongside default SQLite, configured via `database.backend` (`postgres` or `sqlite`).
- Added expanded corporate fundamental and quote attributes to `FundamentalsJob` and `FundamentalsParquetWriter` (`declaration_date`, `next_div_ex_date`, `next_div_pay_date`, `eps_change`, `eps_change_year`, `rev_change_in`, `cusip`, `fund_leverage_factor`, `is_hard_to_borrow`, `is_shortable`, `htb_quantity`, `htb_rate`, `close_price`, `last_price`, `net_change`, `net_percent_change`, and `total_volume`) for once-daily Parquet ingestion in downstream Apache Iceberg tables.
- Added named and inline universe support to DSL fundamentals jobs (`JobBuilder#build_fundamentals_job!`), resolving symbols from `universe :name`, `universe do ... end`, or inline `symbols`.
- Added database abstraction layer (`Tickrake::DB::Adapters`) with `SqliteAdapter` and `PostgresAdapter`, routing `Tracker`, `DataLoader`, rate limiting (`PostgresRateLimiter`), and `tickrake migrate` through the configured backend.
- Added one-time operator script `scripts/import_sqlite_to_postgres.rb` with dry-run support, empty target enforcement, data and sequence verification, and primary key preservation.
- Added option contract fields and ISO 8601 UTC timestamps to `OptionsJob`, `OptionSampleRow`, `OptionSampleWriter`, `DataLoader`, and DuckDB/Parquet compaction: `quote_at` (World Time), `trade_at`, `time_value`, `net_change`, `percent_change`, `settlement_type`, `exercise_type`, `expiration_type`, `multiplier`, and `non_standard`. For historical option samples without `quote_at`, compaction backfills `quote_at` from `sampled_at`.
- Added `storage_byte_delta` signed integer field to successful finalized Parquet write events (`parquet_written`) across candle compaction, option sample compaction, event stream ingestion (`level_one` and `order_book`), economic events, and fundamentals to accurately measure net durable storage added.
- Added intraday options time series and top-of-series publishing to `IntradayPublisherJob`, with incremental uploads, structured `latest` and `series` sections in per-root index JSON, and a daily `clear_at` eviction parameter in the DSL.
- Added consolidated `stream` job type (`StreamJob`, `StreamRunner`, `DSL::StreamBuilder`, `StreamConfig`): allows combining multiple streaming services (Level 1 equities/futures/options, Level 2 order books, and candle chart streams) over a single Schwab WebSocket connection. Dynamically adds (`ADD`) and removes (`UNSUBS`) subscriptions as their individual schedule windows open and close.
- Upgraded `schwab_rb` dependency to `>= 1.0.4`.
- Added diagnostic logging to all runners: structured start/stop events with PID, shutdown reason tracking (SIGTERM, SIGINT, clean_exit), and optional Schwab/tickrake database open events in `ClientFactory` and `DB.connection`.
- Added `start_date` and `end_date` support to maintenance DSL jobs for explicit date-range-scoped maintenance runs.
- Added one-shot job execution: omitting the `schedule` block from a DSL job runs it once and exits. Batch jobs like `metadata_sync` loop until drained. Streaming jobs (`level_one`, `order_book`) require a schedule.
- Added `tickrake prune-orphaned` command to remove `file_metadata_cache` rows whose files no longer exist on disk. Supports `--dry-run` to preview removals without deleting.
- Added size-based staging file rotation (`rotation_size`, `rotation_size_mb`) to `EventsWriter` and DSL streaming jobs (`level_one`, `order_book`, `stream`), enabling files to rotate when either file size exceeds a threshold or the rotation interval elapses.
- Added structured `byte_count` emission for market-data write boundaries across option snapshot CSV finalization, stream NDJSON staging rotation, stream ingestion Parquet writes, and compacted/direct Parquet files (option samples, candles, economic events, and fundamentals).
- Added `universe` support to `stream` job subscriptions (`StreamSubscriptionBuilder`), allowing subscriptions like `level_one`, `order_book`, and `chart_stream` to resolve symbols directly from configured universe files instead of requiring inline symbol lists.

### Changed
- Disabled S3 uploads across maintenance jobs, events ingestion, and reconciler, keeping Tickrake focused on local collection and compaction while preserving MinIO intraday publishing.
- Unified `SqliteRateLimiter` and `PostgresRateLimiter` into a single, model-backed `Tickrake::RateLimiter` using transactional locking.
- Replaced custom SQL database adapters (`Tickrake::DB::Adapters`) and hand-rolled SQL migration runner with `Tickrake::DB::ConnectionManager` and ActiveRecord's `MigrationContext`.
- Streamlined `Tickrake::Tracker` to delegate all persistence and querying to ActiveRecord models while maintaining full compatibility with existing public APIs.
- Defaulted option sample compaction to Parquet only (`artifacts: %w[parquet]`). Compacting to CSV is now optional and can be explicitly requested via `artifacts: [:csv, :parquet]` or `artifacts: [:csv]` in the maintenance DSL or configuration.
- Updated `DuckdbOptionCompactedWriter` to accept optional `csv_path` and `parquet_path`, and updated `Validator` to validate against compacted Parquet files directly using DuckDB when CSV is not present.
- Changed option sample maintenance archival to archive only compacted Parquet files (`artifacts: [parquet]`) to remote S3 storage by default, omitting compacted CSV uploads.

### Removed
- Removed legacy incremental SQLite migration files (001-014) and duplicate PostgreSQL schema script in favor of unified ActiveRecord migrations.
- Removed custom `SqliteAdapter`, `PostgresAdapter`, `Adapters`, and separate `sqlite_rate_limiter.rb` / `postgres_rate_limiter.rb`.

### Fixed
- Fixed `FundamentalsJob` sidecars writing `fetch_run.symbol` as `null`, which caused PostgreSQL `NOT NULL` constraint violations in `MetadataSyncJob`. Stored the literal `none` sentinel on new fundamentals fetch runs, normalized legacy pending null-symbol sidecars to `none` during metadata sync ingestion, and added fallback handling in `Tracker#insert_fetch_runs` and `Tracker#record_start`.
- Fixed option compaction aborting on mixed legacy and expanded CSV headers within the same day (`OptionCompactionDataset` and `DuckdbOptionCompactedWriter`), enabling seamless compaction across schema deployment boundaries while rejecting unsupported or duplicate headers and preserving raw snapshot safety.
- Fixed `OptionsJob#fetch_success` events to assign `market_date` from the collection/sample date in Tickrake's configured market timezone rather than the option expiration date, while retaining `expiration_date` as its own field.
- Fixed `StreamJob` reconnect subscription state: cleared `@active_subscriptions` on session reconnect and teardown so all in-window subscriptions are re-subscribed on the replacement client.
- Fixed `LevelOneJob` stream stall: added a liveness watchdog that tracks `last_event_at` by local receipt time, logs a `stream_stale` event when no data arrives within a configurable timeout (default 60s), stops the dead stream, and reconnects with exponential backoff (up to 10 attempts, capped at 120s delay).
- Fixed `JobRunner` routing so daily-scheduled jobs (`at` + `weekdays`) are dispatched to scheduler runners instead of running once and exiting. Previously only interval-windowed jobs were treated as scheduled.
- Fixed order book event handler to use direct hash access (`event["content"]`, numeric field indices) instead of `.try()` which depends on ActiveSupport and silently swallows missing keys.
- Fixed `ScheduledRunResult#successful?` to treat zero-task runs (`success_count=0, failure_count=0`) as successful instead of degraded. This prevents holiday or off-hours iterations from triggering the consecutive failure counter when no option expirations are available.

### Added
- Added `collection_id` to `fetch_runs` and `file_metadata_cache` (migration 008) so all fetches from one `OptionsJob` iteration are grouped by a single stamped identifier (e.g. `options-20260823T154210Z`). (#61)
- Added index builder infrastructure: `RootIndexBuilder`, `TickersIndexBuilder`, `Publisher`, `AtomicJsonWriter`, `WriteLock`, and `UriBuilder` for generating and safely writing per-root `ROOT.json` and provider-level `tickers.json` index files. (#62)
- Added historical index publishing: `MaintenanceJob` now calls `Publisher#publish` after each successful archive step, writing updated `ROOT.json` and `tickers.json` locally and uploading both to S3. (#63)
- Added intraday index publishing: `OptionsJob` now calls `IntradayPublisher#publish` after each scheduler iteration, writing an updated `ROOT.json` with the current-day intraday section when all expected expirations for a root were successfully fetched. (#64)
- Added a massive options importer for bulk options collection.
- Added support for manual configured jobs and importer progress reporting.
- Added `Tickrake::DataLoader` as a public cache-backed Ruby API for loading stored candle and option data.
- Added typed `Tickrake::DataLoader` rows so numeric fields no longer come back as raw CSV strings.
- Added an opt-in `order: :sample_time_asc` mode to `Tickrake::DataLoader` for chronological candle and option snapshot loading.
- Added `options.snapshot_filename_timezone` and a Schwab filename migration script for UTC option snapshot naming.
- Added canonical S&P 500 index-data generation, SQLite import, and `query --type members` support keyed to current API-queryable tickers.
- Added an explicit `tickrake migrate` command for applying SQLite schema changes.
- Added `tickrake sync-metadata` to insert missing candle metadata cache rows from stored history files.
- Added `maintenance` jobs and option-sample compaction into per-day CSV/parquet artifacts tracked in `file_metadata_cache`.
- Added manual maintenance runs over explicit date ranges, with date-level progress reporting for compaction jobs.
- Added `tickrake validate-option-compaction` to verify a compacted daily options CSV against its source snapshot files before cleanup.
- Added `tickrake delete-compacted-option-samples` for validated raw-snapshot cleanup, with `--dry-run` and source metadata-row removal.
- Added optional `storage.s3_archive` config plus `tickrake archive-compacted-option-samples` to mirror compacted option artifacts into S3 before raw snapshot cleanup.
- Added provider-level scheduled-job resilience settings so Schwab schedulers can auto-restart after repeated failures.
- Added SQLite-backed token bucket rate limiter (`SqliteRateLimiter`) consuming one token per Schwab API call, coordinated across containers via WAL-mode SQLite. Rate limit capacity and refill rate are fully configured via `rate_limit_max_requests` and `rate_limit_interval_seconds` on the provider definition.
- Added reusable maintenance universes loaded from config or separate YAML files so one maintenance job can target many option roots.

### Removed
- Removed `serialize_scheduled_jobs` provider setting and flock-based provider iteration lock (`with_provider_iteration_locks`), which did not work across Docker containers on macOS Docker Desktop. Replaced by the SQLite rate limiter.

### Fixed
- Fixed intraday index to show the most recent file per expiration date across multiple job iterations in the same day, rather than listing all files from the most recent collection. (#65)
- Fixed `ClientFactory#build` to construct a new Schwab client on every call so that refreshed OAuth tokens are always picked up, rather than reusing a stale client instance across scheduler iterations. (#66)
- Fixed IBKR provider hangs on early-date fetches and improved symbol period resolution.
- Stabilized metadata writes during massive imports.
- Fixed option queries to stay on SQLite metadata lookups for large caches instead of falling back to filesystem discovery.
- Fixed MCP tool schemas to avoid invalid empty `required` arrays under newer `mcp` gem validation.
- Fixed option-compaction validation flows to require an explicit provider instead of falling back to `default_provider`.
- Fixed compacted option parquet artifacts to persist numeric and timestamp columns with typed parquet schemas instead of writing every field as a string.
- Fixed maintenance safety so raw source CSVs are only deleted after compaction validation succeeds, and local compacted artifacts are only deleted after successful archive verification for that artifact.

### Changed
- Updated contributor and agent documentation.
- Switched daily option-sample compaction to DuckDB-backed CSV ingest and artifact export for faster compacted CSV/parquet generation.
- Reworked option snapshot storage into dated provider folders and added a one-off migration script for legacy flat paths.
- Normalized S&P 500 index storage around `ticker_id` foreign keys and `ticker_aliases` rows keyed by the current accepted ticker.
- Changed Tickrake to require explicit database migrations instead of running them automatically when the tracker opens the SQLite database.
- Moved Tickrake logs into `~/.tickrake/logs/` and added 14-day log-family retention on top of size-based rotation with 5 files per log family.
- Reworked maintenance jobs from single `task/settings` entries into ordered `tasks:` pipelines with `compact` and `archive` steps for option-sample maintenance.

## [0.3.0] - 2026-04-26

### Changed
- Moved query scans onto SQLite-backed metadata instead of filesystem-only scanning.
- Added runtime database migrations and metadata query indexes to support faster queries.
- Added restartable background schedulers and dual candle scheduling.

## [0.2.0] - 2026-04-19

### Added
- Added option query sorting and limits, plus per-symbol and job-level provider selection.
- Added a Tickrake MCP server, storage stats command, direct one-off run arguments, and a restart command for background jobs.
- Added progress bars for one-off runs and provider symbol mapping for canonical futures storage.

### Changed
- Refactored runtime configuration around config-defined jobs.

## [0.1.3] - 2026-04-13

### Changed
- Wrote option samples through Tickrake-managed storage paths instead of delegating storage layout externally.

## [0.1.2] - 2026-04-13

### Changed
- Switched option expiration handling to use Schwab expiration data objects directly.

## [0.1.1] - 2026-04-13

### Fixed
- Validated option expirations before fetching chains to fail earlier on invalid requests.

## [0.1.0] - 2026-04-09

### Added
- Initial Tickrake gem with CLI, logging, local runtime layout, and gem release workflow.
- Added background job commands, scheduler job registry, and log rotation.
- Added broker-agnostic candle provider seams, IBKR candle support, named provider selection, and a query engine for stored market data.

### Changed
- Refactored candle downloads and options sampling around `schwab_rb`, including provider-based storage configuration updates.
