# Tickrake

`Tickrake` is a releaseable Ruby gem for scheduled market-data collection. It currently
fetches data through `schwab_rb`, stores datasets in Tickrake-managed directories, and
tracks fetch activity in SQLite.

## Install

Install Tickrake as a global gem:

```bash
gem install tickrake
```

Tickrake requires:
- Ruby 3.1+
- `schwab_rb >= 0.9.0`
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

## Config

Run `tickrake init` to generate the default config in `~/.tickrake/`, then edit the
universes, DTE buckets, schedule windows, worker limits, and retry policy.

Tickrake currently supports:

- `provider: schwab`

Set the provider at the top level of the config:

```yaml
provider: schwab
```

For storage, prefer setting `storage.data_dir` and let Tickrake derive the history and
options roots from it:

```yaml
storage:
  data_dir: ~/.tickrake/data
```

That produces provider-separated output paths like:

- `~/.tickrake/data/history/schwab/SPY_day.csv`
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
