# Tickrake: Scheduled Market-Data Collection

`Tickrake` is a Ruby gem designed for scheduled market-data collection (candles and options). It supports multiple providers (currently Schwab and IBKR), stores datasets in managed directories as CSV files, and tracks fetch activity in a SQLite database.

## Project Overview

- **Core Technologies:** Ruby 3.1+, `schwab_rb` (Schwab API), `ib-api` (IBKR API), `mcp` (Model Context Protocol), SQLite3.
- **Architecture:** 
    - **CLI:** Primary interface for initialization, job management, and manual runs.
    - **MCP Server:** Provides a tool-based interface for LLMs and other MCP clients.
    - **Jobs:** Two main types: `candles` (historical price data) and `options` (option chain snapshots).
    - **Storage:** Data is stored in CSV format under `~/.tickrake/data/`, organized by provider and type.
    - **Metadata:** A SQLite database (`~/.tickrake/tickrake.sqlite3`) caches dataset summaries and tracks fetch history.
    - **Concurrency:** Uses threads and `max_workers` config for parallel data fetching.

## Building and Running

### Prerequisites
- Ruby 3.1+
- Bundler

### Installation
```bash
bundle install
```

### Key Commands
- **Initialization:** `bundle exec exe/tickrake init`
- **Validation:** `bundle exec exe/tickrake validate-config`
- **Run a Job:** `bundle exec exe/tickrake run --job JOB_NAME`
- **Background Schedulers:**
    - Start: `bundle exec exe/tickrake start --job all`
    - Status: `bundle exec exe/tickrake status`
    - Stop: `bundle exec exe/tickrake stop --job all`
- **Query Data:** `bundle exec exe/tickrake query --ticker SPY --type candles`
- **MCP Server:** `bundle exec exe/tickrake_mcp`

### Testing
- **Run all tests:** `bundle exec rspec`
- **Run specific test:** `bundle exec rspec spec/path_to_spec.rb`

## Development Conventions

- **Skills:** Use the `skillex` skill to pull any specialized skills needed for development tasks.
- **Pattern:** Follows a standard Ruby gem structure. Logic is encapsulated in `lib/tickrake/`.
- **Providers:** New providers should inherit from `Tickrake::Providers::Base` and be registered in `Tickrake::ProviderFactory`.
- **Jobs:** Scheduled tasks are defined in `lib/tickrake/candles_job.rb` and `lib/tickrake/options_job.rb`.
- **Storage:** CSV writing logic is centralized in `lib/tickrake/storage/`.
- **Configuration:** Uses YAML for configuration (`~/.tickrake/tickrake.yml`). Validation logic is in `lib/tickrake/config_loader.rb`.
- **Migrations:** SQLite schema is managed via additive migrations in `lib/tickrake/db/migrations/`.
- **Testing:** Uses RSpec. Ensure new features have corresponding specs in `spec/`.

## Storage Layout
- **Config:** `~/.tickrake/tickrake.yml`
- **Database:** `~/.tickrake/tickrake.sqlite3`
- **Logs:** `~/.tickrake/*.log`
- **Candles:** `~/.tickrake/data/history/<provider>/<ticker>_<frequency>.csv`
- **Options:** `~/.tickrake/data/options/<provider>/<ticker>_exp<date>_<timestamp>.csv`
