# Tickrake Broker-Agnostic Implementation Plan

## Goal

Move Tickrake from a Schwab-coupled collector to a broker-agnostic market-data collector while keeping the first implementation small, practical, and grounded in the current codebase.

The immediate objective is not to support every broker feature. The immediate objective is to make Tickrake own storage and reconciliation while fetching market data through a provider interface that can be implemented by Schwab first and IB next.

## Current Seams To Reuse

Tickrake already has a useful composition seam:

- `Tickrake::Runtime` owns a `client_factory`
- jobs obtain a client from that factory
- jobs own the scheduling, retry, timeout, and tracker lifecycle

Current broker coupling lives in three places:

1. `Tickrake::ClientFactory`
   - hardcodes Schwab auth and returns a raw Schwab client
2. `Tickrake::CandlesJob`
   - calls `SchwabRb::PriceHistory::Downloader.resolve`
3. `Tickrake::OptionsJob`
   - calls `SchwabRb::OptionSample::Downloader.resolve`

These downloader usages must be removed entirely. Tickrake should keep using broker library client methods for fetches, but must stop depending on Schwab-owned persistence helpers.

Current storage coupling lives in config defaults:

- history defaults to `~/.schwab_rb/data/history`
- options defaults to `~/.schwab_rb/data/options`

## Phase 1: Introduce Tickrake-Owned Provider Interface

Add a Tickrake-local provider abstraction without extracting a new gem yet.

Suggested structure:

- `Tickrake::Providers::Base`
- `Tickrake::Providers::Schwab`
- later `Tickrake::Providers::Ibkr`

Initial provider surface should cover current Tickrake jobs only:

- `fetch_bars(symbol:, frequency:, start_date:, end_date:, extended_hours:, previous_close:)`
- `fetch_option_chain(symbol:, expiration_date:, option_root: nil, fetched_at: Time.now)`
- `capabilities`

The provider should return normalized in-memory data, not write files.

## Phase 2: Define Tickrake Canonical Data Models

Add simple normalized data objects inside Tickrake.

Suggested first-pass types:

- `Tickrake::Data::Bar`
  - `datetime`
  - `open`
  - `high`
  - `low`
  - `close`
  - `volume`
  - optional `source`
  - optional `symbol`
  - optional `frequency`

- `Tickrake::Data::OptionSampleRow`
  - `contract_type`
  - `symbol`
  - `description`
  - `strike`
  - `expiration_date`
  - `mark`
  - `bid`
  - `bid_size`
  - `ask`
  - `ask_size`
  - `last`
  - `last_size`
  - `open_interest`
  - `total_volume`
  - `delta`
  - `gamma`
  - `theta`
  - `vega`
  - `rho`
  - `volatility`
  - `theoretical_volatility`
  - `theoretical_option_value`
  - `intrinsic_value`
  - `extrinsic_value`
  - `underlying_price`
  - optional `source`
  - optional `fetched_at`

Normalize all times to UTC.

## Phase 3: Move Persistence Into Tickrake

Add Tickrake-owned persistence helpers.

Suggested structure:

- `Tickrake::Storage::Paths`
- `Tickrake::Storage::CsvWriter`
- `Tickrake::Storage::CandleReconciler`
- `Tickrake::Storage::OptionSampleWriter`

Responsibilities:

- compute output paths from Tickrake config
- write canonical headers
- write rows in deterministic order
- own merge policy

### Candle persistence requirements

The candle reconciler should preserve current behavior now handled by Schwab:

- one canonical file per symbol and frequency
- read existing CSV if present
- merge with newly fetched bars
- deduplicate by `datetime`
- sort ascending by `datetime`
- rewrite atomically to avoid corruption

Implementation note:

Use write-to-temp plus rename so partial writes do not damage the canonical file.

### Option persistence requirements

The simpler first version can keep snapshot files rather than merged history.

Suggested first step:

- one file per symbol and expiration per fetch timestamp
- deterministic path naming under Tickrake-owned storage
- explicit CSV header owned by Tickrake

Later, if needed, add reconciliation or partitioning rules for options.

## Phase 4: Update Tickrake Config

Adjust config so Tickrake owns storage explicitly.

### Config model changes

Add:

- `data_dir`
- `history_dir`
- `options_dir`
- `provider`

Optional later:

- `provider_settings`
- `raw_data_dir`
- `include_provider_in_path`

### Default values

Move defaults to Tickrake-owned paths:

- `~/.tickrake/data/history`
- `~/.tickrake/data/options`

Migration note:

Keep support for existing configured paths so current users can opt into the new defaults without breaking old installs immediately.

## Phase 5: Replace Raw Schwab Client Factory With Provider Factory

Replace `Tickrake::ClientFactory` with a provider-oriented factory.

Suggested rename:

- `Tickrake::ProviderFactory`

Behavior:

- read configured provider from Tickrake config
- initialize Schwab provider or IB provider
- return an object implementing the provider interface

The current `Runtime` seam can stay mostly unchanged if `client_factory` becomes `provider_factory`, or if the existing name remains temporarily but now returns a provider rather than a broker client.

## Phase 6: Refactor Jobs To Use Providers And Tickrake Storage

### CandlesJob

Change flow from:

- build Schwab client
- call Schwab downloader
- receive file path from Schwab

To:

- build provider
- provider fetches bars
- Tickrake reconciles and writes canonical candle CSV
- tracker records Tickrake output path

### OptionsJob

Change flow from:

- build Schwab client
- call Schwab option downloader
- receive file path from Schwab

To:

- build provider
- provider fetches normalized option sample rows
- Tickrake writes snapshot CSV
- tracker records Tickrake output path

## Phase 7: Implement Schwab Provider First

Build `Tickrake::Providers::Schwab` using existing `schwab_rb` client fetch methods, but not Schwab-owned file-writing helpers.

This provider should:

- initialize a Schwab client the same way Tickrake does today
- fetch candle data through `SchwabRb::Client` methods such as `get_price_history`
- map results into `Tickrake::Data::Bar`
- fetch option chain or sample data through `SchwabRb::Client` methods such as `get_option_chain`
- map results into `Tickrake::Data::OptionSampleRow`
- remove all use of `SchwabRb::PriceHistory::Downloader` and `SchwabRb::OptionSample::Downloader` from Tickrake

This is the safest first milestone because it preserves current broker coverage while decoupling storage.

## Example: Using ib-api In Tickrake

The long-term goal is that Tickrake jobs should talk to a provider, not directly to `ib-api`.

Example Tickrake-side usage:

```ruby
provider = Tickrake::Providers::Ibkr.new(config)
bars = provider.fetch_bars(
  symbol: "SPX",
  frequency: "5min",
  start_date: Date.new(2026, 4, 1),
  end_date: Date.new(2026, 4, 2),
  extended_hours: false,
  previous_close: false
)

output_path = Tickrake::Storage::CandleReconciler.new(config).write(
  provider: "ibkr",
  symbol: "SPX",
  frequency: "5min",
  bars: bars
)
```

Under the hood, the IB provider would use `ib-api` to connect to TWS or IB Gateway, subscribe to incoming messages, issue a historical data request, collect the returned bars, normalize them into `Tickrake::Data::Bar`, and return them to Tickrake for persistence.

Minimal `ib-api` shape inside the provider:

```ruby
ib = IB::Connection.new(host: host, port: port, client_id: client_id, connect: true, received: true)

request_id = 1
collected = Queue.new

historical = ib.subscribe(IB::Messages::Incoming::HistoricalData) do |message|
  next unless message.request_id == request_id

  bars = message.results.map do |bar|
    Tickrake::Data::Bar.new(
      datetime: bar.time.utc.iso8601,
      open: bar.open,
      high: bar.high,
      low: bar.low,
      close: bar.close,
      volume: bar.volume,
      source: "ibkr"
    )
  end

  collected << bars
end

ib.send_message(
  IB::Messages::Outgoing::RequestHistoricalData.new(
    request_id: request_id,
    contract: IB::Index.new(symbol: "SPX", exchange: "CBOE", currency: "USD"),
    end_date_time: "",
    duration: "1 D",
    bar_size: "5 mins",
    what_to_show: :trades,
    use_rth: 1,
    keep_up_todate: 0
  )
)

bars = collected.pop
ib.unsubscribe(historical)
ib.disconnect
```

That example is intentionally narrow: Tickrake should use `ib-api` as a broker client library and convert the returned messages into Tickrake-owned data objects. Tickrake should not leak TWS message classes outside the provider boundary.

## Phase 8: Add IB Provider

Build `Tickrake::Providers::Ibkr` on top of `ib-api` after the provider and storage boundaries are stable.

Keep the IB provider narrow at first:

- historical bars for the frequencies Tickrake needs
- option chain or option quote sampling sufficient to populate Tickrake's option sample schema

Do not try to expose the full IB subscription model in Tickrake's first provider interface.

If later needed, add a separate live-stream collection interface.

## Capability Model

Tickrake should not assume all providers are equally rich.

Add a simple capability object or hash, for example:

- `supports_frequency?(frequency)`
- `supports_option_greeks?`
- `supports_open_interest?`
- `supports_extended_hours_bars?`
- `supports_live_streams?`

That lets Tickrake use richer IB data later without forcing fake parity today.

## Path And Naming Strategy

Tickrake should preserve the existing Schwab filename conventions and use broker separation at the directory level.

### Preserve existing filenames

Candles:

- `SYMBOL_INTERVAL.csv`
- example: `SPX_1min.csv`

Option samples:

- `ROOT_expYYYY-MM-DD_YYYY-MM-DD_HH-MM-SS.csv`
- example: `SPXW_exp2026-02-02_2026-01-21_11-31-43.csv`

### Multi-broker directory layout

Recommended first pass:

- `history/<provider>/SYMBOL_INTERVAL.csv`
- `options/<provider>/ROOT_expYYYY-MM-DD_YYYY-MM-DD_HH-MM-SS.csv`

This avoids collisions when the same symbol is collected from more than one broker while preserving continuity with the filenames already produced through Schwab workflows.

## Operational Considerations

- use atomic writes for canonical files
- log provider name with each job
- include provider in tracker metadata
- store UTC timestamps consistently
- decide whether to retain raw payloads for debugging
- keep current config backward compatible during migration

## Recommended Delivery Order

1. Add Tickrake-owned docs and confirm requirements.
2. Add provider interface and normalized data models.
3. Add Tickrake-owned storage helpers and candle reconciler.
4. Update config defaults and config parsing.
5. Refactor `CandlesJob` to use provider plus storage.
6. Implement Schwab provider.
7. Refactor `OptionsJob` to use provider plus storage.
8. Implement Tickrake-owned option snapshot writing.
9. Add provider capability flags.
10. Implement IB provider.

## Implementation Kickoff Plan

The docs are directionally right, but the codebase suggests a slightly different starting cut if we want to reduce risk:

1. land the provider and storage seams first without changing runtime scheduling behavior
2. migrate candles before options because candle reconciliation is the harder persistence concern
3. keep `Runtime` stable and inject a provider factory behind the existing seam before renaming things
4. preserve config compatibility while introducing Tickrake-owned defaults and provider-aware paths

That leads to the following first implementation sequence.

### Step 1: Add provider and data-model skeletons

Add new files:

- `lib/tickrake/providers/base.rb`
- `lib/tickrake/providers/schwab.rb`
- `lib/tickrake/provider_factory.rb`
- `lib/tickrake/data/bar.rb`
- `lib/tickrake/data/option_sample_row.rb`

Implementation notes:

- Keep the provider interface narrow and job-shaped.
- Make the Schwab provider accept either a prebuilt client or a small auth config object so job specs can stub it cleanly.
- Have `ProviderFactory#build` return a provider object, not a raw broker client.
- Keep `Runtime` compatible by adding `provider_factory` first and optionally aliasing `client_factory` during the transition.

Acceptance criteria:

- no job uses provider methods yet
- Tickrake can construct a Schwab provider from config
- normalized `Bar` and `OptionSampleRow` objects exist and are unit-testable

### Step 2: Add Tickrake-owned storage primitives

Add new files:

- `lib/tickrake/storage/paths.rb`
- `lib/tickrake/storage/csv_writer.rb`
- `lib/tickrake/storage/candle_reconciler.rb`
- `lib/tickrake/storage/option_sample_writer.rb`

Implementation notes:

- `Storage::Paths` should own provider-separated directories:
  - `history/<provider>/SYMBOL_INTERVAL.csv`
  - `options/<provider>/ROOT_expYYYY-MM-DD_YYYY-MM-DD_HH-MM-SS.csv`
- `CandleReconciler` should encapsulate read/merge/dedupe/sort/atomic rewrite.
- `OptionSampleWriter` can remain snapshot-only in v1.
- Keep CSV headers defined in Tickrake, not inferred from broker payloads.

Acceptance criteria:

- canonical candle files can be written and rewritten atomically
- overlapping bars are deduplicated by UTC timestamp
- option rows can be written with Tickrake-owned headers and filenames

### Step 3: Expand config without breaking existing installs

Touch existing files:

- `lib/tickrake/config.rb`
- `lib/tickrake/config_loader.rb`
- `config/tickrake.example.yml`

Config changes:

- add `provider`
- add `data_dir`
- retain `history_dir` and `options_dir`, but resolve them from `data_dir` when not explicitly set
- change defaults to Tickrake-owned storage

Migration behavior:

- explicit legacy `storage.history_dir` and `storage.options_dir` continue to win
- default provider should be `"schwab"` for now
- path expansion stays centralized in `PathSupport`

Acceptance criteria:

- old configs still load
- new configs can omit per-dataset dirs and rely on `data_dir`
- tests pin both legacy and new default behavior

### Step 4: Migrate `CandlesJob` end-to-end

Touch existing files:

- `lib/tickrake/candles_job.rb`
- `lib/tickrake/runtime.rb`

Implementation notes:

- replace `SchwabRb::PriceHistory::Downloader.resolve` with:
  - provider fetch
  - Tickrake reconcile/write
  - tracker finish using Tickrake output path
- replace the current `history_path` helper with `Storage::Paths`
- keep retry, timeout, scheduling, and lookback logic in the job for now

Why candles first:

- `CandlesJob` currently depends on Schwab for both fetching and canonical path resolution
- the existing lookback behavior already expects a single canonical file, so it is the right place to validate the new storage layer

Acceptance criteria:

- jobs still record one fetch run per symbol/frequency
- existing lookback logic works against Tickrake-owned files
- no `SchwabRb::PriceHistory::Downloader` references remain

### Step 5: Migrate `OptionsJob` end-to-end

Touch existing files:

- `lib/tickrake/options_job.rb`

Implementation notes:

- replace `SchwabRb::OptionSample::Downloader.resolve` with provider fetch plus `OptionSampleWriter`
- keep the existing queueing and worker model intact
- record provider-owned output paths in tracker metadata

Acceptance criteria:

- no `SchwabRb::OptionSample::Downloader` references remain
- options snapshots land under Tickrake-owned storage
- current parallel queue processing still works

### Step 6: Add tracker and observability follow-ups

Touch existing files:

- `lib/tickrake/tracker.rb`

Recommended follow-up:

- add a nullable `provider` column to `fetch_runs`
- include provider in `record_start`
- log provider name from both jobs

This can land in the same PR as the job migrations or immediately after. It should not block the provider/storage cut.

## First PR Recommendation

The cleanest first implementation PR is:

1. provider interface skeleton
2. data models
3. storage path and candle reconciler
4. config additions
5. `CandlesJob` migration

That is the smallest slice that proves the new architecture on the hardest persistence path while avoiding a large multi-job refactor in one change set.

Defer `OptionsJob` to the next PR unless the candle migration is unusually small.

## Test Plan

The current spec suite already exercises config loading, tracker writes, and both jobs. Extend that shape instead of inventing a new test harness.

Add or update specs for:

- `spec/config_loader_spec.rb`
  - default provider
  - Tickrake-owned default data paths
  - legacy explicit path compatibility
- `spec/jobs_spec.rb`
  - candles job writes through Tickrake storage, not Schwab downloaders
  - lookback logic uses Tickrake canonical candle path
  - options job writes through Tickrake storage in the follow-up PR
- new storage specs
  - merge and dedupe overlapping bars
  - stable ascending timestamp sort
  - atomic rewrite path behavior
- new provider specs
  - Schwab payloads normalize into `Tickrake::Data::Bar`
  - Schwab option chain data normalize into `Tickrake::Data::OptionSampleRow`

## Open Decisions To Settle Before Coding

These are the only design choices that need to be locked before implementation starts:

1. whether `data_dir` is the single canonical config input with derived dataset dirs, or just a convenience default
2. whether the provider name is always included in output paths or can be toggled later
3. whether `Runtime` should expose both `client_factory` and `provider_factory` during the migration, or switch immediately
4. whether tracker schema changes ship with the first migration PR or immediately after

My recommendation:

- make `data_dir` canonical for defaults only
- always include provider in the path now
- add `provider_factory` and leave `client_factory` untouched until both jobs migrate
- defer tracker schema expansion until after candles are migrated successfully

## Risks To Watch

- accidentally re-encoding Schwab-specific assumptions into the provider interface
- losing current candle merge behavior during migration
- making option schema too narrow for IB data or too wide for clean CSV output
- forcing provider parity where it does not exist
- mixing trading concerns into Tickrake too early

## First Concrete Refactor Targets

The first concrete code targets in Tickrake should be:

- `lib/tickrake/client_factory.rb`
- `lib/tickrake/candles_job.rb`
- `lib/tickrake/options_job.rb`
- `lib/tickrake/config.rb`
- `lib/tickrake/config_loader.rb`

The first concrete migration rule is:

- replace downloader-helper calls with direct `SchwabRb::Client` fetch calls
- move all CSV writing and candle reconciliation into Tickrake

Those are the places where provider construction, Schwab coupling, and storage ownership currently converge.
