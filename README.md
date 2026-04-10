# Tickrake

`Tickrake` is a releaseable Ruby gem for scheduled market-data collection on top of
`schwab_rb`. It keeps payload storage in the standard Schwab CLI directories while
tracking fetch activity in SQLite.

## Commands

```bash
bundle exec exe/tickrake init
bundle exec exe/tickrake validate-config
bundle exec exe/tickrake start options
bundle exec exe/tickrake start candles
bundle exec exe/tickrake status
bundle exec exe/tickrake stop options
bundle exec exe/tickrake stop candles
bundle exec exe/tickrake stop all
bundle exec exe/tickrake logs
bundle exec exe/tickrake logs --tail 100
bundle exec exe/tickrake run options
bundle exec exe/tickrake run candles
bundle exec exe/tickrake run candles --from-config-start
bundle exec exe/tickrake run options --job
bundle exec exe/tickrake run candles --job
bundle exec exe/tickrake run options --verbose
```

## Storage

- Candle payloads: `~/.schwab_rb/data/history`
- Option payloads: `~/.schwab_rb/data/options`
- Tickrake config: `~/.tickrake/tickrake.yml`
- Tickrake metadata DB: `~/.tickrake/tickrake.sqlite3`
- Tickrake log file: `~/.tickrake/tickrake.log`
- Tickrake job state: `~/.tickrake/jobs/*.json`
- Tickrake lockfiles: `~/.tickrake/*.lock`

## Config

Run `tickrake init` to generate the default config in `~/.tickrake/`, then edit the
universes, DTE buckets, schedule windows, worker limits, and retry policy.

Tickrake's candle workflow depends on `schwab_rb >= 0.8.1`, which provides the shared
price-history downloader used for cache-aware candle merges.

For candle collection, each symbol uses a `frequencies:` array. Supported values are `minute`, `5min`, `10min`, `15min`,
`30min`, `day`, `week`, and `month`.

`candles.lookback_days` controls the normal recurring candle request window for
existing files. If a symbol/frequency has no existing CSV yet, Tickrake uses the
configured `start_date` instead. Use `tickrake run candles --from-config-start`
when you want to force a full backfill from the configured `start_date` even if a
history file already exists.

All commands write structured logs to `~/.tickrake/tickrake.log`. Add `--verbose`
to also mirror log output to the console while the command runs.

## Background Jobs

Use `tickrake start options` and `tickrake start candles` to launch the schedulers as
background processes. Tickrake records process metadata in `~/.tickrake/jobs/` and uses
the main log file at `~/.tickrake/tickrake.log` for all scheduler output.

Use `tickrake status` to see whether the `options` and `candles` jobs are running, and
`tickrake stop options`, `tickrake stop candles`, or `tickrake stop all` to request a
graceful shutdown. The long-running runners trap `TERM` and `INT`, finish the current
iteration, and then exit.

Use `tickrake logs` to print the full log file or `tickrake logs --tail 100` to inspect
just the most recent lines.
