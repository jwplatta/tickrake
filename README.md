# Tickrake

[![CI](https://github.com/jwplatta/tickrake/actions/workflows/ci.yml/badge.svg)](https://github.com/jwplatta/tickrake/actions/workflows/ci.yml)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)](https://github.com/jwplatta/tickrake/actions/workflows/ci.yml)
[![Lint](https://img.shields.io/badge/lint-passing-brightgreen.svg)](https://github.com/jwplatta/tickrake/actions/workflows/ci.yml)

Scheduled market-data collection for options, candles, and streaming quotes. Jobs are defined as Ruby DSL scripts and run as Docker containers or standalone processes.

## Install

```bash
gem install tickrake
```

Requires Ruby 3.1+, `schwab_rb >= 1.0.4`, and optionally `ib-api ~> 972.5` for IBKR.

## Setup

Initialize Tickrake's home directory and config:

```bash
tickrake init
tickrake validate-config
tickrake migrate
```

Edit `~/.tickrake/tickrake.yml` to configure providers, universes, storage paths, and timezone.

### Environment Variables

| Variable | Required | Description |
|---|---|---|
| `SCHWAB_API_KEY` | Yes | Schwab API key |
| `SCHWAB_APP_SECRET` | Yes | Schwab API secret |
| `SCHWAB_APP_CALLBACK_URL` | Auth only | Only needed for initial login or token refresh |
| `TICKRAKE_CONFIG` | No | Path to config file (default: `~/.tickrake/tickrake.yml`) |
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

For batch jobs like `metadata_sync`, one-shot mode loops until all pending work is drained. Streaming jobs (`level_one`, `order_book`) require a schedule.

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
  end
end
```

The `dte` method accepts a single value, a range, or an explicit array: `dte 0`, `dte(1..10)`, `dte [0, 1, 3, 7]`.

#### Candles

Fetch historical price candles. Requires a provider, symbols, frequencies, and a start date.

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

The `lookback` controls the recurring request window for existing files. If no file exists yet, the configured `start_date` is used.

#### Streaming (Level One / Order Book)

Stream real-time quotes or order book data via Schwab's websocket API. Events are staged as NDJSON files and later ingested by an events ingest job.

```ruby
Tickrake.job "es_level_one" do
  provider :schwab
  symbols "/ES", "/NQ"

  schedule do
    every 1.seconds
    weekdays from: "08:30", to: "15:00"
  end

  level_one do
    services [:LEVELONE_FUTURES]
    rotation_interval 900
  end
end
```

Available level one services: `LEVELONE_EQUITIES`, `LEVELONE_OPTIONS`, `LEVELONE_FUTURES`, `LEVELONE_FUTURES_OPTIONS`, `LEVELONE_FOREX`.

Order book jobs follow the same pattern with `order_book do ... end` and services like `NYSE_BOOK`, `NASDAQ_BOOK`, or `OPTIONS_BOOK`.

#### Consolidated Stream (Multiplexed)

Schwab allows only **one WebSocket streamer connection per account**. To avoid connection conflicts across containers, define a single `stream` job combining multiple services, symbols, and schedule windows over one shared connection:

```ruby
Tickrake.job "market_streams" do
  provider :schwab
  stale_timeout 60

  level_one "equities" do
    symbols "SPY", "QQQ", "IWM"
    services [:level_one_equities]
    rotation_interval 300
    schedule do
      weekdays from: "08:30", to: "15:00"
    end
  end

  level_one "futures" do
    symbols "/ES"
    services [:level_one_futures]
    rotation_interval 300
    schedule do
      days %w[sun mon tue wed thu], from: "17:00", to: "23:59"
      days %w[mon tue wed thu fri], from: "00:00", to: "16:00"
    end
  end

  order_book "equity_books" do
    symbols "SPY", "QQQ", "IWM"
    services [:nyse_book, :nasdaq_book]
    rotation_interval 300
    schedule do
      weekdays from: "08:30", to: "15:00"
    end
  end

  chart_stream "futures_candles" do
    symbols "/ES"
    services [:chart_futures]
    flush_interval 60
    schedule do
      days %w[sun mon tue wed thu], from: "17:00", to: "23:59"
      days %w[mon tue wed thu fri], from: "00:00", to: "16:00"
    end
  end
end
```

The runner keeps the connection open while any subscription is in-window, dynamically sending `ADD` when a subscription's window opens and `UNSUBS` when it closes without dropping the stream.

#### Events Ingest

Reads staged NDJSON event files from streaming jobs, converts them to Parquet, and uploads to a configured datastore.

```ruby
Tickrake.job "events_ingestor" do
  schedule do
    every 60.seconds
    every_day from: "08:00", to: "17:30"
  end

  events_ingest do
    batch_size 20
    datastore :s3_archive
  end
end
```

#### Metadata Sync

Processes pending metadata written by collection jobs and upserts it into the SQLite metadata cache.

```ruby
Tickrake.job "metadata_sync" do
  schedule do
    every 30.seconds
    weekdays from: "08:00", to: "16:00"
  end

  metadata_sync do
    batch_size 500
  end
end
```

#### Intraday Publisher

Publishes the latest option chain samples as CSV files and JSON indexes to a datastore (typically Minio) for downstream consumers.

```ruby
Tickrake.job "intraday_publisher" do
  schedule do
    every 60.seconds
    weekdays from: "08:30", to: "15:30"
  end

  intraday_publish do
    datastore :minio_intraday
  end
end
```

#### Reconciler

Validates that collected data matches expectations and reports gaps.

```ruby
Tickrake.job "reconciler" do
  schedule do
    at "16:00"
    weekdays
  end

  reconcile do
    providers :schwab
  end
end
```

#### Archival

Compacts raw option sample CSVs into single-day files and archives them to S3. Runs post-close to consolidate the day's snapshots into compacted CSV and Parquet artifacts, upload them to long-term storage, and clean up local source files.

```ruby
Tickrake.job "postclose_archival" do
  provider :schwab

  schedule do
    at "18:05"
    weekdays
  end

  maintenance do
    compact :option_samples, universe: "index_option_roots", delete_sources: true
    archive :option_samples, universe: "index_option_roots",
            to: :s3_archive,
            artifacts: [:csv, :parquet],
            retain: { csv: false, parquet: true }
  end
end
```

Raw source CSVs are never deleted unless compaction validation succeeds. Local artifacts are never deleted unless their archive upload and remote verification succeeds.

### Scheduling

The `schedule` block supports two styles:

**Recurring** — run at a fixed interval within time windows:

```ruby
schedule do
  every 5.seconds
  weekdays from: "08:30", to: "15:00"
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
- `days [:mon, :wed, :fri], from: "HH:MM", to: "HH:MM"`

Duration helpers: `5.seconds`, `30.seconds`, `1.minutes`, `30.minutes`, `1.hours`, `90.days`.

Times are interpreted in the timezone configured in `tickrake.yml`.

### Universes

Universes are named lists of symbols defined in `tickrake.yml`. Jobs reference them by name:

```ruby
universe "spx_symbols"
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

## Running Jobs

### With Docker

Each job runs as its own container. Set `TICKRAKE_JOB_FILE` to the path of the DSL script inside the container:

```yaml
# docker-compose.yml
services:
  spx_0dte_options:
    image: tickrake:latest
    env_file:
      - env/secrets.env
      - env/prod.env
    environment:
      TICKRAKE_JOB_FILE: /jobs/spx_0dte_options.rb
    volumes:
      - ./jobs:/jobs:ro
      - ~/.tickrake:/root/.tickrake
      - ~/.schwab_rb:/root/.schwab_rb
    restart: unless-stopped
```

### Without Docker

Run a job file directly:

```bash
ruby jobs/spx_0dte_options.rb
```

Or use the CLI for one-off runs:

```bash
tickrake run --type options --provider schwab --ticker '$SPX' --expiration-date 2026-04-11 --option-root SPXW
tickrake run --type candles --provider schwab --ticker SPY --start-date 2026-04-01 --end-date 2026-04-11 --frequency 30min
```

## Storage

```
~/.tickrake/
├── tickrake.yml                    # config
├── tickrake.sqlite3                # metadata cache
├── logs/                           # per-job rotating logs
│   ├── spx_0dte_options.log
│   └── stock_options.log
└── data/
    ├── history/<provider>/         # candle CSVs
    │   └── SPY_day.csv
    └── options/<provider>/YYYY/MM/DD/
        ├── SPXW_exp2026-04-11_2026-04-11_10-30-00.csv   # raw snapshots
        ├── SPXW_samples_2026-04-11.csv                   # compacted
        └── SPXW_samples_2026-04-11.parquet               # compacted
```

Configure storage in `tickrake.yml`:

```yaml
storage:
  data_dir: ~/.tickrake/data
  s3_archive:
    bucket: tickrake
    region: us-east-1
    prefix:
    storage_class: GLACIER_IR
```

## Configuration Reference

### Providers

```yaml
default_provider: schwab
providers:
  schwab:
    adapter: schwab
    settings:
      rate_limit_max_requests: 120
      rate_limit_interval_seconds: 60
      restart_after_consecutive_failures: 3
      restart_cooldown_seconds: 30
  ibkr-paper:
    adapter: ibkr
    settings:
      host: 127.0.0.1
      port: 4002
      client_id: 1001
```

### Provider Precedence

When multiple providers are configured, resolution order is:

1. CLI `--provider` flag
2. Per-symbol `provider:` in the universe
3. Job-level `provider`
4. Global `default_provider`

### Logging

Each job writes to a rotating log file at `~/.tickrake/logs/<job_name>.log` (5 files, 10 MB each, 14-day retention). Set `TICKRAKE_LOG_STDOUT=1` to also emit logs to stdout for Docker log aggregation.
