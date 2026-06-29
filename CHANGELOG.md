# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added
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
- Added provider-level scheduled-job resilience settings so Schwab schedulers can serialize runs and auto-restart after repeated failures.

### Fixed
- Fixed IBKR provider hangs on early-date fetches and improved symbol period resolution.
- Stabilized metadata writes during massive imports.
- Fixed option queries to stay on SQLite metadata lookups for large caches instead of falling back to filesystem discovery.
- Fixed MCP tool schemas to avoid invalid empty `required` arrays under newer `mcp` gem validation.
- Fixed option-compaction validation flows to require an explicit provider instead of falling back to `default_provider`.
- Fixed compacted option parquet artifacts to persist numeric and timestamp columns with typed parquet schemas instead of writing every field as a string.

### Changed
- Updated contributor and agent documentation.
- Reworked option snapshot storage into dated provider folders and added a one-off migration script for legacy flat paths.
- Normalized S&P 500 index storage around `ticker_id` foreign keys and `ticker_aliases` rows keyed by the current accepted ticker.
- Changed Tickrake to require explicit database migrations instead of running them automatically when the tracker opens the SQLite database.
- Moved Tickrake logs into `~/.tickrake/logs/` and added 14-day log-family retention on top of size-based rotation with 5 files per log family.

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
