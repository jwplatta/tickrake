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
- options universe
- candle universe
- DTE buckets
- options monitor interval
- options windows
- EOD candle time

Run a one-off command to verify the setup:

```bash
tickrake run options --verbose
tickrake run candles --verbose
tickrake query --provider schwab
```

## Commands

```bash
tickrake init
tickrake validate-config
tickrake start options
tickrake start candles
tickrake status
tickrake stop options
tickrake stop candles
tickrake stop all
tickrake logs cli
tickrake logs options --tail 100
tickrake logs candles --tail 100
tickrake run options
tickrake run candles
tickrake run candles --from-config-start
tickrake run options --job
tickrake run candles --job
tickrake run options --verbose
tickrake query --provider schwab
tickrake query --type candles --provider ibkr-paper --ticker SPY
tickrake query --type options --provider schwab --ticker '$SPX' --format json
```

## Storage

- Market data root: `~/.tickrake/data`
- Candle payloads: `~/.tickrake/data/history/<provider>`
- Option payloads: `~/.tickrake/data/options/<provider>`
- Tickrake config: `~/.tickrake/tickrake.yml`
- Tickrake metadata DB: `~/.tickrake/tickrake.sqlite3`
- Tickrake CLI log: `~/.tickrake/cli.log`
- Tickrake options log: `~/.tickrake/options.log`
- Tickrake candles log: `~/.tickrake/candles.log`
- Tickrake job state: `~/.tickrake/jobs/*.json`
- Tickrake lockfiles: `~/.tickrake/*.lock`

The SQLite database is migrated additively at startup. Tickrake creates missing tables
or adds missing columns, but it does not recreate or overwrite the existing database.

## Config

Run `tickrake init` to generate the default config in `~/.tickrake/`, then edit the
universes, DTE buckets, schedule windows, worker limits, and retry policy.

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

Then select which configured provider to use on each command:

```bash
tickrake run candles --provider ibkr-paper
tickrake run options --provider schwab
tickrake start candles --provider ibkr-paper
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
- `--start-date YYYY-MM-DD`
- `--end-date YYYY-MM-DD`
- `--format text|json`

Examples:

```bash
tickrake query --provider ibkr-paper
tickrake query --type candles --provider ibkr-paper --ticker SPY
tickrake query --type candles --provider ibkr-paper --ticker '$SPX' --frequency 30min
tickrake query --type options --provider schwab --ticker '$SPX'
tickrake query --type candles --provider ibkr-paper --ticker SPY --format json
```

Text output is grouped by provider, dataset type, and ticker. Candle summaries include
frequency, row count, available timestamp range, and file path. Option summaries include
snapshot count, available sample range, and the latest snapshot path.

## Provider Status

- `schwab` supports candles and the existing options collection workflow.
- `ibkr` currently supports candle collection only.

If you run with an `ibkr` provider entry, `tickrake run candles --provider NAME`
will use Interactive Brokers historical data through `ib-api`. `tickrake run options`
still requires a `schwab` provider and will raise an error otherwise.

For candle collection, each symbol uses a `frequencies:` array. Supported values are `minute`, `5min`, `10min`, `15min`,
`30min`, `day`, `week`, and `month`.

`candles.lookback_days` controls the normal recurring candle request window for
existing files. If a symbol/frequency has no existing CSV yet, Tickrake uses the
configured `start_date` instead. Use `tickrake run candles --from-config-start`
when you want to force a full backfill from the configured `start_date` even if a
history file already exists.

One-off and direct CLI operational commands write structured logs to `~/.tickrake/cli.log`.
The long-running schedulers write to separate rotating log files:

- `~/.tickrake/options.log`
- `~/.tickrake/candles.log`

Tickrake rotates each log with a fixed-file policy of 10 files at 10 MB each.
Add `--verbose` to one-off commands to also mirror log output to the console while
the command runs.

## Background Jobs

Use `tickrake start options` and `tickrake start candles` to launch the schedulers as
background processes. Tickrake records process metadata in `~/.tickrake/jobs/` and writes
the scheduler output into the job-specific rotating log files.

Use `tickrake status` to see whether the `options` and `candles` jobs are running, and
`tickrake stop options`, `tickrake stop candles`, or `tickrake stop all` to request a
graceful shutdown. The long-running runners trap `TERM` and `INT`, finish the current
iteration, and then exit.

Use `tickrake logs cli`, `tickrake logs options`, or `tickrake logs candles` to print
the relevant log stream, and add `--tail 100` to inspect just the most recent lines.
