# Tickrake

`Tickrake` is a releaseable Ruby gem for scheduled market-data collection on top of
[`schwab_rb`](/Users/jplatta/repos/schwab_rb). It keeps payload storage in the standard
Schwab CLI directories while tracking fetch activity in SQLite.

## Commands

```bash
bundle exec exe/tickrake validate-config --config config/tickrake.yml
bundle exec exe/tickrake run options-monitor --config config/tickrake.yml
bundle exec exe/tickrake run eod-candles --config config/tickrake.yml
```

## Storage

- Candle payloads: `~/.schwab_rb/data/history`
- Option payloads: `~/.schwab_rb/data/options`
- Tickrake metadata DB: `~/.schwab_rb/data/tickrake.sqlite3`

## Config

Copy `config/tickrake.example.yml` to your own config path and edit the universes,
DTE buckets, schedule windows, worker limits, and retry policy.
