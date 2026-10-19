# Tickrake Agent Guide

Applies to all coding assistants and autonomous agents (Gemini, Claude, Codex, etc.). Tickrake is a Ruby gem for scheduled market data collection (options, candles, level one, order book, fundamentals, economic events) with local filesystem buffering, SQLite/PostgreSQL metadata tracking, and MinIO/S3 staging publishing.

## Workflow

- **Check the worktree before editing.** Do not revert unrelated user changes.
- **Default to [`CONTRIBUTING.md`](CONTRIBUTING.md)** for branch naming, conventional commits, tests, changelog handling, and PR hygiene.
  - **Important**: Treat [`CONTRIBUTING.md`](CONTRIBUTING.md) as a standard guideline and default operating procedure, not a law of nature. When explicitly advised by the user to bypass or adapt guidelines, follow the user's direction.
- **Keep changes targeted.** Prefer small, focused edits and targeted specs over broad, sweeping refactors.
- **Commits & PRs**:
  - Use conventional commit messages (`feat:`, `fix:`, `chore:`, `refactor:`, `build:`).
  - Stage files explicitly by name — avoid accidental `git add -A` or committing untracked artifacts.
  - Do not include automated tool footers (e.g., "Generated with...") in commit or PR descriptions.
- **Shared agent skills**:
  - Use the `skillex` skill to manage shared agent skills. Pull before using or updating a shared skill, and push updates when the skill changes.
  - Use the `project-backlog` skill to log bugs, feature requests, chores, and documentation tasks into `docs/backlog/`.

## Architecture & Current Design

Tickrake operates across three primary tiers:
1. **Collectors / Schedulers (`lib/tickrake/jobs/`, `lib/tickrake/streaming/`)**:
   - **Batch Scrapes**: Options chains (`OptionsJob`), historical candles (`CandlesJob`), economic events (`EconomicEventsJob`), fundamentals (`FundamentalsJob`).
   - **Streaming Streams**: Level 1 quotes and order book depth (`StreamJob`, `EventsWriter`) written to timestamped/rotating `.ndjson` files in `pending_events_dir`.
   - **Supervision**: `SchedulerRunner` variants oversee window scheduling, signal handling, and process supervision.
2. **Metadata & State (`lib/tickrake/db/`, `lib/tickrake/models/`, `lib/tickrake/core/`)**:
   - Schema migrations live under `lib/tickrake/db/migrations/`.
   - Sidecar JSON files (`.meta.json`) record fetch/file status before transaction ingestion via `MetadataSyncJob`.
3. **Staging & Publishing (`lib/tickrake/storage/staging_publisher.rb`)**:
   - `StagingPublisher` scans completed raw files across datasets (`options`, `candles`, `level_one`, `order_book`, `fundamentals`, `economic_events`) and uploads them to MinIO/S3 staging buckets with structured `byte_count` logging.

## Project Rules

- **Targeted edits**: Prefer small classes with single responsibilities over monolithic multi-purpose classes.
- **Additive migrations**: Keep schema changes additive in `lib/tickrake/db/migrations/`. Always register new migrations in `lib/tickrake.rb` and `Tracker.migrations`.
- **Preserve storage conventions**: Preserve on-disk and MinIO dataset naming and directory layouts unless explicitly instructed.
- **Verify cache & runtime behavior**: When modifying collectors, jobs, schedulers, or storage paths, verify both runtime execution and sidecar/metadata-cache behavior.
- **Encapsulate provider logic**: Keep broker/provider-specific logic inside provider or importer classes (`lib/tickrake/providers/`), not scattered through the CLI.

## Key Paths

- `lib/tickrake/cli.rb`: CLI commands, parsing, and execution dispatch.
- `lib/tickrake/core/`: Runtime container, config loaders, symbol normalizers, and scheduler supervisors.
- `lib/tickrake/jobs/`: Batch collection jobs, staging publisher jobs, and scheduler runners.
- `lib/tickrake/streaming/`: Streaming WebSocket client supervision, NDJSON writers, and delivery stats.
- `lib/tickrake/storage/`: Path resolvers (`Paths`), staging publisher (`StagingPublisher`), scanners, and S3/MinIO archive client.
- `lib/tickrake/models/`: ActiveRecord models, data objects, and schema records.
- `lib/tickrake/db/migrations/`: Additive database migrations.
- `spec/`: RSpec test coverage.

## Common Commands

- Install dependencies: `bundle install`
- Initialize local configuration: `bundle exec exe/tickrake init`
- Validate configuration: `bundle exec exe/tickrake validate-config`
- Run test suite: `bundle exec rspec`
- Run focused spec: `bundle exec rspec spec/path/to_spec.rb`

## Storage Layout

- **Config**: `~/.tickrake/tickrake.yml`
- **Metadata Database**: `~/.tickrake/tickrake.sqlite3`
- **Raw Data Directory (`~/.tickrake/data/`)**:
  - **Candles**: `data/history/<provider>/<frequency>/<symbol>.csv`
  - **Options**: `data/options/<provider>/<YYYY>/<MM>/<DD>/<symbol>_exp<date>_<timestamp>.csv`
  - **Level One**: `data/level_one/<provider>/<YYYY>/<MM>/<DD>/<symbol>.ndjson`
  - **Order Book**: `data/order_book/<provider>/<YYYY>/<MM>/<DD>/<symbol>.ndjson`
  - **Fundamentals**: `data/fundamentals/<provider>/<YYYY>/<MM>/<DD>.ndjson`
  - **Economic Events**: `data/economic_events/<source>/<category>/<YYYY>/<MM>/<DD>.ndjson`
- **Pending Buffers**:
  - **Events buffer**: `~/.tickrake/pending_events/*.ndjson`
  - **Metadata sidecars**: `~/.tickrake/pending_metadata/*.meta.json`

## Local Notes

Keep personal or session-specific notes in untracked files such as `AGENTS.local.md` or in `docs/notes/`. Treat committed root docs as shared repository policy.
