# Tickrake

`Tickrake` is a releaseable Ruby gem for scheduled market-data collection on top of
[`schwab_rb`](/Users/jplatta/repos/schwab_rb). It keeps payload storage in the standard
Schwab CLI directories while tracking fetch activity in SQLite.

## Commands

```bash
bundle exec exe/tickrake init
bundle exec exe/tickrake validate-config --config ~/.tickrake/tickrake.yml
bundle exec exe/tickrake run options-monitor --config ~/.tickrake/tickrake.yml
bundle exec exe/tickrake run eod-candles --config ~/.tickrake/tickrake.yml
```

## Storage

- Candle payloads: `~/.schwab_rb/data/history`
- Option payloads: `~/.schwab_rb/data/options`
- Tickrake config: `~/.tickrake/tickrake.yml`
- Tickrake metadata DB: `~/.tickrake/tickrake.sqlite3`
- Tickrake lockfiles: `~/.tickrake/*.lock`

## Config

Run `tickrake init` to generate the default config in `~/.tickrake/`, then edit the
universes, DTE buckets, schedule windows, worker limits, and retry policy.
