# Tickrake

`Tickrake` is a releaseable Ruby gem for scheduled market-data collection. It currently
fetches data through `schwab_rb`, stores datasets in Tickrake-managed directories, and
tracks fetch activity plus cached dataset-summary metadata in SQLite.

## Install

Install Tickrake as a global gem:

```bash
gem install tickrake
```

Tickrake requires:
- Ruby 3.1+
- `schwab_rb >= 0.9.2`
- `ib-api ~> 972.5` for IBKR candle collection
- exported Schwab credentials in the shell environment
- a valid Schwab token file at `~/.schwab_rb/token.json`

Required environment variables:
- `SCHWAB_API_KEY`
- `SCHWAB_APP_SECRET`

`SCHWAB_APP_CALLBACK_URL` is only needed when you are logging in or refreshing auth setup.

## First Run

Initialize Tickrake's home directory and config:

```bash
tickrake init
tickrake validate-config
```

Then edit:

```text
~/.tickrake/tickrake.yml
```

to set:
- named jobs under `schedule`
- each job's `type`
- job-specific universes
- option job DTE buckets, intervals, and windows
- candle job lookback windows and run times
- optional `manual: true` jobs that are available through config but only run when triggered

Run a one-off command to verify the setup:

```bash
tickrake run --job index_options --verbose
tickrake run --job eod_candles --verbose
tickrake query --provider schwab
```

## Commands

```bash
tickrake init
tickrake validate-config
tickrake start --job index_options
tickrake start --job eod_candles
tickrake restart --job index_options
tickrake restart --job eod_candles
tickrake restart --job all
tickrake status
tickrake stop --job index_options
tickrake stop --job eod_candles
tickrake stop --job all
tickrake logs cli
tickrake logs index_options --tail 100
tickrake logs eod_candles --tail 100
tickrake run --job index_options
tickrake run --job eod_candles
tickrake run --job eod_candles --from-config-start
tickrake run --job manual_candles
tickrake run --job index_options --verbose
tickrake run --type candles --provider ibkr-paper --ticker SPY --start-date 2026-04-01 --end-date 2026-04-11 --frequency minute
tickrake run --type options --provider schwab --ticker '$SPX' --expiration-date 2026-04-11 --option-root SPXW
tickrake query --provider schwab
tickrake query --type candles --provider ibkr-paper --ticker SPY
tickrake query --type options --provider schwab --ticker '$SPX' --format json
tickrake storage-stats
```

## MCP Server

`tickrake` now includes a simple stdio MCP server so MCP clients such as Claude can inspect
the local Tickrake installation without shelling out to the CLI.

Start it with:

```bash
bundle exec exe/tickrake_mcp
```

The initial MCP tool surface is intentionally small and mostly read-only:

- `help_tool`
- `validate_config_tool`
- `status_tool`
- `search_datasets_tool`
- `storage_stats_tool`
- `logs_tool`
- `start_job_tool`
- `stop_job_tool`
- `restart_job_tool`

These tools map to the same underlying library code used by the CLI for config loading,
job inspection, dataset discovery, storage summaries, log access, and scheduler lifecycle
management. `search_datasets_tool` returns dataset metadata only; it does not return raw
market data rows.

Typical workflow:

1. Start the server with `bundle exec exe/tickrake_mcp`.
2. Call `validate_config_tool` to confirm the active config and storage paths.
3. Use `search_datasets_tool` to discover candle files or option snapshots.
4. Use `status_tool`, `logs_tool`, `start_job_tool`, `stop_job_tool`, and `restart_job_tool` to manage the schedulers.

Example MCP calls:

```json
{"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"validate_config_tool","arguments":{}}}
```

```json
{"jsonrpc":"2.0","id":"2","method":"tools/call","params":{"name":"search_datasets_tool","arguments":{"type":"candles","provider":"ibkr-paper","ticker":"SPX","frequency":"all"}}}
```

```json
{"jsonrpc":"2.0","id":"3","method":"tools/call","params":{"name":"search_datasets_tool","arguments":{"type":"options","provider":"schwab","ticker":"SPXW"}}}
```

```json
{"jsonrpc":"2.0","id":"4","method":"tools/call","params":{"name":"status_tool","arguments":{}}}
```

```json
{"jsonrpc":"2.0","id":"5","method":"tools/call","params":{"name":"logs_tool","arguments":{"target":"options"}}}
```

Example Claude Desktop MCP entry:

```json
{
  "mcpServers": {
    "tickrake": {
      "command": "bundle",
      "args": ["exec", "exe/tickrake_mcp"],
      "cwd": "/Users/jplatta/repos/tickrake"
    }
  }
}
```

## Storage

- Market data root: `~/.tickrake/data`
- Candle payloads: `~/.tickrake/data/history/<provider>`
- Option payloads: `~/.tickrake/data/options/<provider>`
- Tickrake config: `~/.tickrake/tickrake.yml`
- Tickrake metadata DB: `~/.tickrake/tickrake.sqlite3`
- Tickrake CLI log: `~/.tickrake/cli.log`
- Tickrake job logs: `~/.tickrake/<job_name>.log`
- Tickrake job state: `~/.tickrake/jobs/*.json`
- Tickrake lockfiles: `~/.tickrake/*.lock`

The SQLite database is migrated additively at startup. Tickrake creates missing tables
or adds missing columns, but it does not recreate or overwrite the existing database.

## Config

Run `tickrake init` to generate the default config in `~/.tickrake/`, then edit the
named jobs under `schedule`, their universes, their timing, and the shared runtime
policy.

Tickrake currently supports these provider adapters:

- `schwab`
- `ibkr`

Configure named providers under `providers:` and choose one as the default:

```yaml
default_provider: schwab
providers:
  schwab:
    adapter: schwab
    settings: {}
  ibkr-paper:
    adapter: ibkr
    settings:
      host: 127.0.0.1
      port: 4002
      client_id: 1001
```

Jobs can set a provider default, and universe entries can still override that per symbol:

```yaml
default_provider: schwab
providers:
  schwab:
    adapter: schwab
    settings: {}
  ibkr-paper:
    adapter: ibkr
    settings:
      host: 127.0.0.1
      port: 4002
      client_id: 1001
schedule:
  index_options:
    type: options
    provider: schwab
    interval_seconds: 300
    windows:
      - days: [mon, tue, wed, thu, fri]
        start: "08:30"
        end: "15:00"
    dte_buckets: [0DTE, 1DTE, 30DTE]
    universe:
      - symbol: $SPX
        option_root: SPXW
      - symbol: SPY
        provider: ibkr-paper
  eod_candles:
    type: candles
    provider: ibkr-paper
    run_at: "16:05"
    days: [mon, tue, wed, thu, fri]
    lookback_days: 7
    universe:
      - symbol: /ES
        provider: schwab
        start_date: "2020-01-01"
        frequencies: [day, 30min, 5min, 1min]
      - symbol: SPY
        start_date: "2020-01-01"
        frequencies: [day]
  spx_min_candles:
    type: candles
    provider: ibkr-paper
    interval_seconds: 120
    windows:
      - days: [mon, tue, wed, thu, fri]
        start: "08:30"
        end: "15:00"
    lookback_days: 7
    universe:
      - symbol: $SPX
        start_date: "2026-03-01"
        frequencies: [30min, 5min, 1min]
  manual_candles:
    type: candles
    manual: true
    provider: ibkr-paper
    lookback_days: 7
    universe:
      - symbol: SPY
        start_date: "2020-01-01"
        frequencies: [day, 1min]
      - symbol: QQQ
        start_date: "2020-01-01"
        frequencies: [day, 1min]
```

Manual jobs stay under `schedule` so they can reuse the same configured universes and
provider precedence as scheduled jobs. Run them with `tickrake run --job JOB_NAME`.
They are not started by `tickrake start --job all` or `tickrake restart --job all`, and
they cannot be launched as background schedulers.

Provider precedence is:
- CLI `--provider`
- per-symbol `provider:`
- job-level `provider:`
- global `default_provider`

You can also still select which configured provider to use on each command:

```bash
tickrake run --job eod_candles --provider ibkr-paper
tickrake run --job index_options --provider schwab
tickrake run --type candles --provider ibkr-paper --ticker SPY --start-date 2026-04-01 --end-date 2026-04-11 --frequency 30min
tickrake run --type options --provider schwab --ticker '$SPX' --expiration-date 2026-04-11 --option-root SPXW
tickrake start --job eod_candles --provider ibkr-paper
tickrake query --provider ibkr-paper
```

For storage, prefer setting `storage.data_dir` and let Tickrake derive the history and
options roots from it:

```yaml
storage:
  data_dir: ~/.tickrake/data
```

That produces provider-separated output paths like:

- `~/.tickrake/data/history/schwab/SPY_day.csv`
- `~/.tickrake/data/history/ibkr-paper/SPY_day.csv`
- `~/.tickrake/data/options/schwab/SPXW_exp2026-04-11_2026-04-11_10-30-00.csv`

If you need a custom layout, you can still set:

```yaml
storage:
  history_dir: /mnt/market-data/history
  options_dir: /mnt/market-data/options
```

When `history_dir` or `options_dir` are set explicitly, they override the derived
subdirectories from `data_dir`. Tickrake still appends the provider name underneath
those roots.

## Querying Stored Data

Use `tickrake query` to inspect the data already persisted on disk without printing raw
rows. Queries are filesystem-backed and use the Tickrake SQLite database as a cache for
summary metadata.

At least one of `--provider` or `--ticker` is required.

Available filters:

- `--type candles|options`
- `--provider NAME`
- `--ticker SYMBOL`
- `--frequency FREQ` for candle queries only
- `--start-date YYYY-MM-DD` filters candle coverage dates or option sample datetimes
- `--end-date YYYY-MM-DD` filters candle coverage dates or option sample datetimes
- `--exp-date YYYY-MM-DD` filters option snapshots by expiration date
- `--limit N` limits option snapshots after filtering
- `--ascending true|false` sorts option snapshots by sample datetime and defaults to `true`
- `--format text|json`

Examples:

```bash
tickrake query --provider ibkr-paper
tickrake query --type candles --provider ibkr-paper --ticker SPY
tickrake query --type candles --provider ibkr-paper --ticker '$SPX' --frequency 30min
tickrake query --type options --provider schwab --ticker '$SPX'
tickrake query --type options --provider schwab --ticker 'SPXW' --start-date 2026-03-30 --end-date 2026-03-30 --exp-date 2026-04-06
tickrake query --type options --provider schwab --ticker 'SPXW' --exp-date 2026-04-06 --limit 5 --ascending false
tickrake query --type candles --provider ibkr-paper --ticker SPY --format json
```

Text output is grouped by provider, dataset type, and ticker. Candle summaries include
frequency, row count, available timestamp range, and file path. Option summaries list
each matching snapshot with its root symbol, expiration date, sample datetime, and file
path.

## Storage Stats

Use `tickrake storage-stats` for a capacity view of what is currently on disk.

It reports:

- total stored data files and total bytes across history plus options
- per-dataset totals for `history` and `options`
- per-provider file counts and bytes
- average file size plus oldest and newest file timestamps
- largest files in each dataset root
- SQLite metadata DB size
- combined log-file footprint

Example:

```bash
tickrake storage-stats
tickrake storage-stats --config /mnt/tickrake/tickrake.yml
```

## Provider Status

- `schwab` supports candles and the existing options collection workflow.
- `ibkr` currently supports candle collection only.

If you run a candles job or direct candles request with an `ibkr` provider entry,
Tickrake uses Interactive Brokers historical data through `ib-api`. Options jobs and
direct options requests still require a `schwab` provider and will raise an error
otherwise.

For candle collection, each symbol uses a `frequencies:` array. Supported values are `minute`, `5min`, `10min`, `15min`,
`30min`, `day`, `week`, and `month`.

For one-off direct candle fetches, you can bypass the configured candle universe and run a
single request with `--ticker`, `--start-date`, `--end-date`, and `--frequency`.
For one-off direct option fetches, you can bypass the configured options universe with
`--ticker` and `--expiration-date`, plus optional `--option-root`.

Each `type: candles` job has its own `lookback_days`, which controls the normal recurring
candle request window for existing files. If a symbol/frequency has no existing CSV yet,
Tickrake uses the configured `start_date` instead. Use `tickrake run --job JOB_NAME
--from-config-start` when you want to force a full backfill from the configured
`start_date` even if a history file already exists.

Candle jobs support two schedule styles:

- daily collection with `run_at` plus `days`
- recurring collection inside market windows with `interval_seconds` plus `windows`

One-off and direct CLI operational commands write structured logs to `~/.tickrake/cli.log`.
Configured jobs write to separate rotating log files named after the job key:

- `~/.tickrake/index_options.log`
- `~/.tickrake/eod_candles.log`

Tickrake rotates each log with a fixed-file policy of 10 files at 10 MB each.
Add `--verbose` to one-off commands to also mirror log output to the console while
the command runs.

## Background Jobs

Use `tickrake start --job JOB_NAME` to launch configured schedulers as background
processes. Tickrake records process metadata in `~/.tickrake/jobs/` and writes scheduler
output into job-specific rotating log files.

Use `tickrake status` to see configured jobs plus any orphaned/stale registry entries, and
`tickrake stop --job JOB_NAME` or `tickrake stop --job all` to request a graceful
shutdown. The long-running runners trap `TERM` and `INT`, finish the current iteration,
and then exit.

Use `tickrake restart --job JOB_NAME` or `tickrake restart --job all` to stop and
relaunch background jobs. Restart reuses the last recorded config path, provider, and
candle backfill flag for that job unless you pass explicit flags such as `--config`,
`--provider`, or `--from-config-start`.

Use `tickrake logs cli` or `tickrake logs JOB_NAME` to print the relevant log stream, and
add `--tail 100` to inspect just the most recent lines.
