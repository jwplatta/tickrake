# Feature Requests

- [ ] To distinguish future ticker symbols, let's append the carrot to the front of the symbol so `/ES` would be `^ES` and let's use this for the filenames so, for example, ES futures on the SP500 doesn't get confused with the stock `ES`. We will have to normalize these future symbols for both schwab and ibkr. So for example, we will need to use the schwab valid symbol `/ES` when making requests, but we will want to write that data to files that have the `^ES` symbol appended to the front. I don't know hte ibkr approved symbol.
- [ ] We want to be able to have a default provider for a job, e.g. the default for the candles is ibkr-paper, but then we also want to be able to optionally specify a provider for a specific ticker which will override the default. For example:
```yaml
timezone: America/Chicago
sqlite_path: ~/.tickrake/tickrake.sqlite3

default_provider: ibkr-paper
providers:
  schwab:
    adapter: schwab
    settings: {}
  ibkr-paper:
    adapter: ibkr
    settings:
      host: 127.0.0.1
      port: 7497
      client_id: 1001
      historical_timeout_seconds: 120

...

options:
  dte_buckets:
    - 0DTE
    - 2DTE
  universe:
    - symbol: $SPX
      option_root: SPXW
      provider: schwab
candles:
  lookback_days: 7
  universe:
    - symbol: /ES
      provider: schwab
      start_date: "2020-01-01"
      frequencies: [day, 30min, 5min, 1min]
```

- [ ] We want to be able to define multiple option sample jobs that have different sampling intervals. For example I have two options monitors here for index options and stock options with the stock options getting sampled at a lover frequency. I think this makes sense, but maybe there's a better way to structure it:
```yaml
timezone: America/Chicago
sqlite_path: ~/.tickrake/tickrake.sqlite3

default_provider: ibkr-paper
providers:
  schwab:
    adapter: schwab
    settings: {}
  ibkr-paper:
    adapter: ibkr
    settings:
      host: 127.0.0.1
      port: 7497
      client_id: 1001
      historical_timeout_seconds: 120
  ibkr-live:
    adapter: ibkr
    settings:
      host: 127.0.0.1
      port: 7496
      client_id: 1000
      historical_timeout_seconds: 120

storage:
  data_dir: ~/.tickrake/data

schedule:
  options_monitors:
    index_options:
      interval_seconds: 300
      windows:
        - days: [mon, tue, wed, thu, fri]
          start: "08:30"
          end: "15:05"
    stock_options:
      interval_seconds: 1800
      windows:
        - days: [mon, tue, wed, thu, fri]
          start: "08:30"
          end: "15:05"
runtime:
  max_workers: 4
  retry_count: 2
  retry_delay_seconds: 2
  option_fetch_timeout_seconds: 30
  candle_fetch_timeout_seconds: 1800

index_options:
  dte_buckets:
    - 0DTE
    - 1DTE
    - 2DTE
    - 3DTE
    - 4DTE
    - 5DTE
    - 6DTE
    - 7DTE
    - 8DTE
    - 9DTE
    - 10DTE
    - 30DTE
  universe:
    - symbol: $SPX
      option_root: SPXW
    - symbol: $VIX
    - symbol: $NDX
    - symbol: $RUT
    - symbol: XSP
    - symbol: SPY
    - symbol: QQQ
    - symbol: IWM
    - symbol: DIA

stock_options:
  dte_buckets:
    - 0DTE
    - 1DTE
    - 2DTE
    - 3DTE
    - 4DTE
    - 5DTE
    - 6DTE
    - 7DTE
    - 8DTE
    - 9DTE
    - 10DTE
    - 30DTE
  universe:
    - symbol: TSLA
    - symbol: PLTR
    - symbol: MSFT
    - symbol: MRVL
    - symbol: INTC
    - symbol: PCOR
    - symbol: AMD
    - symbol: NVDA
    - symbol: AAPL
```