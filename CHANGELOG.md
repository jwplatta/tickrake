# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added
- Added a massive options importer for bulk options collection.
- Added support for manual configured jobs and importer progress reporting.
- Added `Tickrake::DataLoader` as a public cache-backed Ruby API for loading stored candle and option data.
- Added typed `Tickrake::DataLoader` rows so numeric fields no longer come back as raw CSV strings.

### Fixed
- Fixed IBKR provider hangs on early-date fetches and improved symbol period resolution.
- Stabilized metadata writes during massive imports.
- Fixed option queries to stay on SQLite metadata lookups for large caches instead of falling back to filesystem discovery.
- Fixed MCP tool schemas to avoid invalid empty `required` arrays under newer `mcp` gem validation.

### Changed
- Updated contributor and agent documentation.
- Reworked option snapshot storage into dated provider folders and added a one-off migration script for legacy flat paths.

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
