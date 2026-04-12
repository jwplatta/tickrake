# Broker-Agnostic Market Data Direction for Tickrake

## Project Description

Tickrake should become a broker-agnostic market-data collection gem that owns the full collection pipeline for persisted datasets. Its responsibility is to schedule jobs, fetch market data through broker-specific adapters, normalize the results into Tickrake-defined data models, reconcile and persist files, and track collection metadata. Broker gems such as `schwab_rb` and `ib-api` should be responsible only for broker communication, authentication or session handling, and fetching data from the underlying broker APIs.

In this model, Tickrake stops delegating storage concerns to `schwab_rb`. Instead, Tickrake defines a market data provider interface internally for now, with broker adapters behind it. `schwab_rb` and an eventual IB adapter become implementations of that provider. Tickrake then takes over file naming, storage locations, CSV writing, merge and reconciliation behavior, retry behavior, and dataset tracking. This keeps the collector product broker-agnostic and prevents its persistence model from being coupled to one broker gem's CLI and storage conventions. Tickrake should continue to call broker library APIs for data fetching, but persistence must be wholly Tickrake-owned.

## Requirements

- Tickrake must define an internal broker-agnostic `MarketDataProvider` interface.
- The provider interface should initially cover the current Tickrake use cases:
  - candle and history fetches
  - option chain or option sample fetches
  - quote snapshots later if needed
- Tickrake must own persistence of scraped market data.
- `schwab_rb` and `ib-api` integrations should return in-memory data or normalized objects, not file paths.
- Tickrake must use broker library client methods for fetching data, not broker CLI workflows or broker-owned downloader helpers that also write files.
- For Schwab specifically, Tickrake should use `SchwabRb::Client` fetch methods and remove all usage of `SchwabRb::PriceHistory::Downloader` and `SchwabRb::OptionSample::Downloader`.
- Tickrake must define its own normalized data model for persisted datasets rather than persisting Schwab-shaped or IB-shaped payloads directly.
- Tickrake config must explicitly define storage roots for data outputs instead of relying on `schwab_rb` defaults like `~/.schwab_rb/data/history` and `~/.schwab_rb/data/options`.
- Tickrake must preserve existing candle reconciliation behavior:
  - a single canonical file per symbol and interval
  - append and merge without overwriting good data
  - deduplicate overlapping candles
  - stable sort by timestamp
- Candle reconciliation should remain a Tickrake concern regardless of broker.
- Option sample persistence should also move into Tickrake, but can initially remain simpler than candles if there is no existing merge logic.
- Tickrake should continue tracking fetch jobs and outputs in SQLite and metadata, but output paths should now point to Tickrake-managed storage.
- The design should allow later extraction of the market data provider abstraction into a shared gem if both Tickrake and OptionsTrader need it.

## Current Data Shapes

Current persisted shapes imply two different storage strategies.

### Candle CSV shape

Current candle files under `~/.schwab_rb/data/history` look like:

```csv
datetime,open,high,low,close,volume
2026-02-09T14:30:00Z,6917.26,6919.4,6906.01,6910.98,0
```

This is already close to a broker-neutral canonical format. Tickrake should preserve this simple UTC OHLCV shape and own reconciliation of new rows into an existing file.

### Option sample CSV shape

Current option sample files under `~/.schwab_rb/data/options` look like:

```csv
contract_type,symbol,description,strike,expiration_date,mark,bid,bid_size,ask,ask_size,last,last_size,open_interest,total_volume,delta,gamma,theta,vega,rho,volatility,theoretical_volatility,theoretical_option_value,intrinsic_value,extrinsic_value,underlying_price
CALL,SPXW  260202C02800000,SPXW 02/02/2026 2800.00 C,2800.0,2026-02-02,4021.25,4012.2,1,4030.3,1,0.0,0,4,0,1.0,0.0,-0.1,0.006,0.932,137.19,29.0,4018.734,4019.18,-4019.18,6819.18
```

This is a wide snapshot schema. Tickrake should define and own this schema explicitly, including which fields are canonical, which fields are optional by provider, and whether snapshots are appended, replaced, or stored one file per fetch.

## File Naming Conventions

Tickrake should preserve the existing Schwab file naming conventions when it takes over persistence.

### Candle filenames

Candle files should continue to use:

- `SYMBOL_INTERVAL.csv`

Examples:

- `SPX_1min.csv`
- `VIX_5min.csv`

### Option sample filenames

Option sample files should continue to use:

- `ROOT_expYYYY-MM-DD_YYYY-MM-DD_HH-MM-SS.csv`

Example:

- `SPXW_exp2026-02-02_2026-01-21_11-31-43.csv`

### Multi-broker collision rule

Tickrake should preserve these filenames exactly, but avoid collisions by separating providers at the directory level rather than changing the filenames.

Recommended first-pass layout:

- `history/<provider>/SYMBOL_INTERVAL.csv`
- `options/<provider>/ROOT_expYYYY-MM-DD_YYYY-MM-DD_HH-MM-SS.csv`

This preserves continuity with existing Schwab-produced files while allowing Tickrake to support multiple brokers without filename collisions.

## Config Changes

Tickrake config should add explicit storage settings under Tickrake control.

At minimum:

- `data_dir`
- `history_dir`
- `options_dir`

Reasonable defaults:

- `~/.tickrake/data/history`
- `~/.tickrake/data/options`

Optional future settings:

- `provider`
- `provider_settings`
- `file_format`
- `path_scheme`
- `raw_data_dir`

## Candle Reconciliation

The existing Schwab candle downloader reconciles bars for a symbol and interval into a single file without overwriting valid data or producing duplicates. Tickrake should preserve this behavior when it takes ownership of persistence.

Required behavior:

- maintain one canonical file per symbol and interval
- merge new bars into existing files
- remove duplicates by canonical timestamp key
- sort deterministically by timestamp ascending
- avoid destructive overwrite of good history
- keep reconciliation logic independent of provider

This should become a Tickrake concern rather than a Schwab concern.

## Option Sample Persistence

Option sample persistence also needs to move into Tickrake, but it does not need to match the candle merge model immediately.

Open questions to resolve in implementation:

- should option files remain snapshots keyed by fetch time or gain a later reconciliation model
- should there be a normalized superset schema with nullable columns for broker-specific gaps

## Additional Considerations

- Define canonical schemas now for candles and option samples.
- Separate fetching from normalization from persistence.
- Add provider capability flags so Tickrake can detect richer providers such as IB.
- Normalize timestamps to UTC internally.
- Keep broker identity at the directory level and preserve the established Schwab filename conventions.
- Decide whether raw broker payload retention is useful for debugging and backfills.
- Keep broker quirks out of job orchestration code.
- Keep the abstraction in Tickrake for now and extract later only after it stabilizes across multiple consumers.
