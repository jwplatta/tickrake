# Tickrake

[![CI](https://github.com/jwplatta/tickrake/actions/workflows/ci.yml/badge.svg)](https://github.com/jwplatta/tickrake/actions/workflows/ci.yml)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)](https://github.com/jwplatta/tickrake/actions/workflows/ci.yml)
[![Lint](https://img.shields.io/badge/lint-passing-brightgreen.svg)](https://github.com/jwplatta/tickrake/actions/workflows/ci.yml)

Scheduled market-data collection for options chains, historical and streaming candles, Level 1 quotes, Level 2 order books, corporate fundamentals, and economic events, with local filesystem buffering, SQLite/PostgreSQL metadata tracking, and S3-compatible / MinIO staging and intraday publishing. Jobs are defined as Ruby DSL scripts and run as Docker containers or standalone processes.

## Install

```bash
gem install tickrake
```

Requires Ruby 3.1+, `schwab_rb >= 1.0.6`, and optionally `ib-api ~> 972.5` for IBKR.

## Setup

Initialize Tickrake's home directory, validate configuration, and run database migrations:

```bash
tickrake init
tickrake validate-config
tickrake migrate
```

Edit `~/.tickrake/tickrake.yml` to configure database settings, target datastores, providers, universes, storage paths, and timezone. Tickrake automatically loads environment variables via `Dotenv` (from `--env-file`, `TICKRAKE_ENV_FILE`, `./.env`, or `~/.tickrake/.env`) and interpolates ERB (`<%= ENV.fetch(...) %>`) in both `tickrake.yml` and universe YAML files.

### Database Backend

Tickrake uses ActiveRecord with SQLite by default (`~/.tickrake/tickrake.sqlite3`) and supports PostgreSQL:

```yaml
database:
  adapter: postgresql # or sqlite3
  host: localhost
  port: 5432
  database: tickrake
  user: tickrake
  password: secret
  # Or connection URL:
  # url: postgres://user:password@localhost:5432/tickrake
```

Run `tickrake migrate` to apply schema migrations to the configured backend.

To perform a one-time migration of existing SQLite state into PostgreSQL, use the operator script:

```bash
bundle exec ruby scripts/import_sqlite_to_postgres.rb --dry-run
bundle exec ruby scripts/import_sqlite_to_postgres.rb
```

### Environment Variables

| Variable | Required | Description |
|---|---|---|
| `SCHWAB_API_KEY` | Yes | Schwab API key |
| `SCHWAB_APP_SECRET` | Yes | Schwab API secret |
| `SCHWAB_APP_CALLBACK_URL` | Auth only | Only needed for initial login or token refresh |
| `TICKRAKE_CONFIG` | No | Path to config file (default: `~/.tickrake/tickrake.yml`) |
| `TICKRAKE_ENV_FILE` | No | Path to `.env` file loaded before parsing config |
| `TICKRAKE_JOB_FILE` | Docker | Path to a DSL job file for the container to run |
| `TICKRAKE_LOG_STDOUT` | No | Set to `1` to also send logs to stdout |

A valid Schwab token must exist in `~/.schwab_rb/schwab.db` (managed by `schwab_rb`).

## Defining Jobs

Jobs are defined using the `Tickrake.job` DSL. Each job file is a self-contained Ruby script:

```ruby
require "tickrake"

Tickrake.job "my_job" do
  provider :schwab
  # ... job-specific blocks
end
```

Every job requires one typed block that determines the job type. A `schedule` block is required for recurring jobs. When omitted, the job runs once and exits — useful for backfills and ad-hoc tasks:

```ruby
Tickrake.job "drain_metadata" do
  metadata_sync do
    batch_size 1000
  end
end
```

For batch jobs like `metadata_sync`, one-shot mode loops until all pending work is drained. Streaming jobs (`level_one`, `order_book`, `chart_stream`, `stream`) require a schedule.

### Job Types

#### Options

Fetch option chains at a recurring interval. Requires a provider, a universe of symbols, and DTE (days to expiration) buckets.

```ruby
Tickrake.job "spx_short_dated_options" do
  provider :schwab
  universe "spx_symbols"

  schedule do
    every 60.seconds
    weekdays from: "08:31", to: "15:05"
  end

  options do
    dte(1..10)
    batch_size 5
  end
end
```

The `dte` method accepts a single value, a range, or an explicit array: `dte 0`, `dte(1..10)`, `dte [0, 1, 3, 7]`.
The optional `batch_size` setting (default `1`) groups resolved consecutive expiration dates into balanced `from_date..to_date` batches per API request before splitting the response into individual per-expiration CSV snapshots.

#### Candles

Fetch historical price candles into date-partitioned daily CSVs. Requires a provider, symbols or universe, frequencies, and a start date.

```ruby
Tickrake.job "futures_candles" do
  provider :schwab
  symbols "/ES", "/NQ", "/RTY", "/YM"
  lookback 90.days

  schedule do
    at "16:30"
    weekdays
  end

  candles do
    frequencies "day", "30min", "5min", "1min"
    start_date "2026-06-01"
  end
end
```

Supported frequencies: `1min`, `5min`, `10min`, `15min`, `30min`, `day`, `week`, `month`.

The `lookback` controls the recurring request window for existing symbols. If no data exists yet, the configured `start_date` is used.

#### Consolidated Stream (Multiplexed)

Schwab allows only **one WebSocket streamer connection per account**. Define a consolidated `stream` job combining multiple Level 1, Level 2 order book, and chart candle subscriptions over a single shared WebSocket connection:

```ruby
Tickrake.job "market_streams" do
  provider :schwab

  stream do
    stale_timeout 60

    level_one "equity_level_one" do
      universe "equity_symbols"
      services [:level_one_equities]
      schedule { weekdays from: "08:30", to: "15:00" }
    end

    order_book "equity_order_book" do
      universe "equity_symbols"
      services [:NYSE_BOOK, :NASDAQ_BOOK]
      schedule { weekdays from: "08:30", to: "15:00" }
    end

    level_one "spxw_0dte_level_one" do
      services [:level_one_options]
      contracts do
        underlying "SPX"
        option_root "SPXW"
        atm_points 50
        expirations front: 1
        re_resolve_interval 5
      end
      schedule do
        weekdays from: "06:30", to: "08:20"
        weekdays from: "08:30", to: "15:00"
      end
    end

    order_book "spxw_0dte_order_book" do
      services [:OPTIONS_BOOK]
      contracts do
        underlying "SPX"
        option_root "SPXW"
        atm_points 50
        expirations front: 1
        re_resolve_interval 5
      end
      schedule { weekdays from: "08:30", to: "15:00" }
    end

    level_one "futures_level_one" do
      symbols "/ES", "/NQ", "/RTY"
      services [:level_one_futures]
      schedule do
        days %w[sun mon tue wed thu], from: "17:00", to: "23:59"
        days %w[mon tue wed thu fri], from: "00:00", to: "16:00"
      end
    end

    chart_stream "futures_chart_streams" do
      symbols "/ES", "/NQ", "/RTY"
      services [:chart_futures]
      flush_interval 120
      schedule do
        days %w[sun mon tue wed thu], from: "17:00", to: "23:59"
        days %w[mon tue wed thu fri], from: "00:00", to: "16:00"
      end
    end
  end
end
```

The runner keeps the connection open while any subscription is in-window, dynamically sending `ADD` when a subscription's window opens (or when dynamic ATM option contracts roll into new strikes) and `UNSUBS` when it closes without dropping the stream.

Dynamic option contracts (`contracts do ... end`) support:
- `underlying`: Underlying ticker (e.g. `"SPX"`, `"SPY"`, `"QQQ"`)
- `option_root`: Optional explicit root filter (e.g. `"SPXW"` when underlying is `"SPX"`)
- `atm_points` or `atm_strikes`: Strike window around spot price (in index/price points or strike count)
- `expirations front: N`: Number of nearest non-expired expirations to include
- `re_resolve_interval`: Minutes between mid-session ATM contract re-resolutions (additive `ADD` during the window, full `UNSUBS` on window exit)

Standalone `level_one`, `order_book`, and `chart_stream` job blocks are also supported when running a single-service stream.

#### Fundamentals & Economic Events

Collect corporate fundamentals (`fundamentals`) and economic/earnings/FOMC calendar events (`economic_events`):

```ruby
Tickrake.job "fundamentals" do
  provider :schwab
  universe "fundamentals_symbols"

  schedule do
    at "17:00"
    weekdays
  end

  fundamentals do
    batch_size 50
  end
end

Tickrake.job "economic_events" do
  schedule do
    at "06:00"
    weekdays
  end

  economic_events do
    lookback_days 30
    lookahead_days 90
    categories "economic", "earnings", "fomc"
  end
end
```

#### Metadata Sync

Processes pending `.meta.json` sidecar files written by collection jobs and upserts them into the database metadata cache (`fetch_runs` and `file_metadata_cache`).

```ruby
Tickrake.job "metadata_sync" do
  schedule do
    every 30.seconds
    weekdays from: "06:00", to: "16:00"
  end

  metadata_sync do
    batch_size 500
  end
end
```

#### Intraday Publisher

Publishes the latest option chain snapshots and per-root intraday time-series JSON indexes to the configured `intraday_target` bucket (MinIO/S3) for live dashboards and consumers.

```ruby
Tickrake.job "intraday_publisher" do
  schedule do
    every 30.seconds
    weekdays from: "06:30", to: "15:30"
  end

  intraday_publish do
    clear_at "05:00"
  end
end
```

#### Staging Publisher

Scans completed raw dataset files (`options`, `candles`, `level_one`, `order_book`, `fundamentals`, `economic_events`) and uploads them to the configured `staging_target` bucket (MinIO/S3) for downstream lakehouse/Iceberg ingestion.

```ruby
Tickrake.job "staging_publisher" do
  schedule do
    at "17:30"
    weekdays
  end

  staging_publish do
    datasets :all
    batch_size 500
    delete_after_upload true
  end
end
```

### Scheduling

The `schedule` block supports two styles:

**Recurring** — run at a fixed interval within time windows:

```ruby
schedule do
  every 30.seconds
  weekdays from: "06:30", to: "08:20"
  weekdays from: "08:30", to: "15:10"
end
```

**Daily** — run once at a specific time:

```ruby
schedule do
  at "16:30"
  weekdays
end
```

Available window helpers:
- `weekdays` / `weekdays from: "HH:MM", to: "HH:MM"`
- `weekends` / `weekends from: "HH:MM", to: "HH:MM"`
- `every_day` / `every_day from: "HH:MM", to: "HH:MM"`
- `days %w[sun mon tue wed thu], from: "HH:MM", to: "HH:MM"`

Duration helpers: `5.seconds`, `30.seconds`, `1.minutes`, `30.minutes`, `1.hours`, `90.days`.

Times are interpreted in the timezone configured in `tickrake.yml`.

### Universes

Universes are named lists of symbols defined in `tickrake.yml` or external YAML files. Jobs reference them by name:

```ruby
universe "index_symbols"
```

Or define symbols inline for simpler jobs:

```ruby
symbols "/ES", "/NQ", "/RTY"
```

For options jobs, you can also define an inline universe with per-ticker option roots:

```ruby
universe do
  ticker "$SPX", option_root: "SPXW"
  ticker "$SPX", option_root: "SPX"
end
```

## Running Jobs & CLI Commands

### Running DSL Jobs

Run a job file directly:

```bash
ruby jobs/spx_0dte_options.rb
```

Or in Docker Compose by setting `TICKRAKE_JOB_FILE` to the path of the DSL script inside the container:

```yaml
services:
  market_streams:
    image: tickrake:latest
    env_file:
      - envs/secrets.env
      - envs/prod.env
    environment:
      TICKRAKE_CONFIG: /config/tickrake.yml
      TICKRAKE_JOB_FILE: /jobs/prod/market_streams.rb
    volumes:
      - ./services/tickrake/jobs:/jobs:ro
      - ./services/tickrake/config/prod:/config:ro
      - ~/.schwab_rb:/root/.schwab_rb
    restart: unless-stopped
```

### CLI Commands

```bash
# Initialize ~/.tickrake and config template
tickrake init [--config path/to/tickrake.yml] [--force]

# Validate tickrake.yml and referenced universe files
tickrake validate-config [--config path/to/tickrake.yml] [--env-file path/to/.env] [--verbose]

# Run pending ActiveRecord database migrations
tickrake migrate [--config path/to/tickrake.yml] [--env-file path/to/.env]

# Publish completed local dataset files to staging_target
tickrake publish-staging [--dataset TYPE] [--after DATE] [--before DATE] [--batch-size N] [--no-delete]

# Purge dataset files for a specific date from staging_target and/or local disk
tickrake purge-staging --date YYYY-MM-DD [--dataset TYPE] [--provider NAME] [--remote] [--local] [--all] [--dry-run]

# Inspect job or CLI logs
tickrake logs [TARGET] [--tail N]
```

## Storage Layout

```
~/.tickrake/
├── tickrake.yml
├── tickrake.sqlite3
├── logs/
│   ├── market_streams.log
│   └── 0-4dte_options.log
├── staging/
│   └── pending_metadata/*.meta.json
└── data/
    ├── candles/<provider>/<YYYY>/<MM>/<DD>/<frequency>/<symbol>.csv
    ├── options/<provider>/<YYYY>/<MM>/<DD>/<root>_exp<YYYY-MM-DD>_<timestamp>.csv
    ├── level_one/<provider>/<YYYY>/<MM>/<DD>/<symbol>.ndjson
    ├── order_book/<provider>/<YYYY>/<MM>/<DD>/<symbol>.ndjson
    ├── fundamentals/<provider>/<YYYY>/<MM>/<DD>.ndjson
    └── economic_events/<source>/<category>/<YYYY>/<MM>/<DD>.ndjson
```

## Configuration Reference

```yaml
timezone: America/Chicago

database:
  adapter: <%= ENV.fetch('TICKRAKE_DB_ADAPTER', 'sqlite3') %>
  database: <%= ENV.fetch('TICKRAKE_DB_DATABASE', Tickrake::PathSupport.sqlite_path) %>

staging_target:
  type: s3_compatible
  endpoint: <%= ENV.fetch('MINIO_ENDPOINT', 'http://minio:9000') %>
  bucket: <%= ENV.fetch('TICKRAKE_STAGING_BUCKET', 'tickrake-staging') %>
  access_key_id: <%= ENV.fetch('MINIO_ACCESS_KEY', 'minioadmin') %>
  secret_access_key: <%= ENV.fetch('MINIO_SECRET_KEY', 'minioadmin') %>

intraday_target:
  type: s3_compatible
  endpoint: <%= ENV.fetch('MINIO_ENDPOINT', 'http://minio:9000') %>
  bucket: <%= ENV.fetch('TICKRAKE_INTRADAY_BUCKET', 'tickrake-intraday') %>
  access_key_id: <%= ENV.fetch('MINIO_ACCESS_KEY', 'minioadmin') %>
  secret_access_key: <%= ENV.fetch('MINIO_SECRET_KEY', 'minioadmin') %>

default_provider: schwab
providers:
  schwab:
    adapter: schwab
    settings:
      rate_limit_max_requests: 120
      rate_limit_interval_seconds: 60
      restart_after_consecutive_failures: 3
      restart_cooldown_seconds: 30
    symbol_map:
      /ES: ^ES
      /NQ: ^NQ
      /RTY: ^RTY

storage:
  data_dir: <%= ENV.fetch('TICKRAKE_DATA_DIR', '~/.tickrake/data') %>
  candles_dir: ~/.tickrake/data/candles
  options_dir: ~/.tickrake/data/options
  pending_metadata_dir: ~/.tickrake/staging/pending_metadata
```

### Logging

Each job writes to a rotating log file at `~/.tickrake/logs/<job_name>.log` (5 files, 10 MB each, 14-day retention). Set `TICKRAKE_LOG_STDOUT=1` to also emit logs to stdout for Docker log aggregation.
