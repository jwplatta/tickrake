---
type: feature
tags: [fundamentals, parquet, iceberg, schwab, dividends, dsl]
title: Expand fundamentals schema and formalize once-daily Parquet ingestion for Iceberg
description: Add missing dividend, borrow, and growth fields to FundamentalsJob and formalize the once-daily Parquet architecture for downstream Iceberg ingestion
created: 2026-10-14
updated: 2026-10-29
status: complete
priority: high
source: antigravity/tickrake
---

# Summary

Tickrake's `FundamentalsJob` scrapes cross-sectional corporate fundamentals and dividend data from Schwab's REST API (`get_instruments` with `FUNDAMENTAL` projection + `get_quotes` supplement) and outputs a typed Parquet file for each sample date. 

Currently, several high-value fields returned by these endpoints—such as forward dividend dates (`declaration_date`, `next_div_ex_date`, `next_div_pay_date`), CUSIP, fund leverage, borrow/shorting indicators, earnings changes, and day-end closing quote stats—are omitted from the schema.

This task expands the Parquet schema and row builder to capture all available fields without additional network overhead. It also resolves a DSL universe resolution gap and formalizes the once-daily execution model for downstream Apache Iceberg table ingestion.

---

## Architectural Context: Iceberg & Daily Parquet Partitioning

As Tickrake transitions to using **Apache Iceberg** to manage historical archives, metadata catalogs, and table schemas:

1. **Immutable Daily Files as Iceberg Ingestion Units:**
   - Rather than attempting to rewrite or append into a large annual file (`fundamentals_<YYYY>.parquet`) on every scrape (which causes massive S3 object rewrite churn, version bloat, and broken append-only semantics), Tickrake will write **one immutable Parquet file per sample date**:
     `~/.tickrake/data/fundamentals/<provider>/<YYYY>/<MM>/<DD>.parquet`
   - Each daily file contains all universe symbols as rows for that trading day.
   - Each file is uploaded to S3 exactly once per day (252 PUT requests/year, negligible cost).

2. **Downstream Iceberg Responsibility:**
   - Apache Iceberg catalogs will ingest each daily Parquet file as an append-only partition (`sample_date=YYYY-MM-DD`).
   - Long-term archival, schema evolution, and periodic/year-end table compaction (e.g., Iceberg `rewrite_data_files` bin-packing) will be handled natively by Iceberg/query engines (DuckDB/Trino/Spark), leaving Tickrake solely responsible for reliable scraping and clean daily partition outputs.

3. **Strict Separation of Concerns:**
   - **`fundamentals`**: Scheduled strictly **once per day** (post-market close, e.g. 15:15–16:00) to capture day-end financial statements, valuation metrics, and corporate actions.
   - **Intraday Quotes / Pricing**: Any future high-frequency intraday price/quote scraping will reside in a separate job/dataset so it does not pollute the clean daily corporate fundamentals table.

---

## DSL Review & Findings

### 1. Once-Daily Execution Mechanism
`FundamentalsSchedulerRunner` already provides a daily schedule mode via `#due_for_daily_schedule?`:
```ruby
def due_for_daily_schedule?(time)
  return false if @last_run_on == time.to_date
  return false unless @scheduled_job.days.include?(time.strftime("%a").downcase[0, 3])

  current_minutes = (time.hour * 60) + time.min
  target_minutes = (@scheduled_job.run_at[0] * 60) + @scheduled_job.run_at[1]
  current_minutes >= target_minutes
end
```
When configured with `at "15:30"` and `weekdays`, the runner automatically tracks `@last_run_on = now.to_date` upon completion, guaranteeing that the job runs **strictly once per trading day**.

### 2. DSL Gap: Universe Resolution
In `Tickrake::DSL::JobBuilder#build_fundamentals_job!` (`lib/tickrake/dsl/job_builder.rb`):
```ruby
# Current implementation:
def build_fundamentals_job!(schedule)
  ...
  universe: @inline_symbols,
  ...
end
```
* **The Problem:** `build_fundamentals_job!` currently only reads `@inline_symbols` (defined via `symbols "AAPL", "MSFT"`). If a user specifies a named universe (`universe :income_etfs`) or an inline universe block (`universe do ticker "JEPI"; ticker "JEPQ" end`), `@universe_name` and `@universe_builder` are completely ignored, resulting in an empty symbol list at runtime.
* **The Fix:** Update `JobBuilder#build!` to pass `config` into `build_fundamentals_job!(config, schedule)` and resolve symbols using the standard priority:
  ```ruby
  symbol_list = if @universe_name
                  config.universe(@universe_name).entries.map(&:symbol)
                elsif @universe_builder
                  @universe_builder.entries.map(&:symbol)
                else
                  @inline_symbols
                end
  ```

### 3. Target DSL Syntax
Once updated, the target DSL definition for the job will be clean and intuitive:

```ruby
job "daily_fundamentals" do
  provider "schwab"

  schedule do
    at "15:30"
    weekdays
  end

  universe :income_etfs # or symbols "JEPI", "JEPQ"

  fundamentals do
    batch_size 50
  end
end
```

---

## Requirements

### 1. Parquet Schema Expansion
Update `Tickrake::Storage::FundamentalsParquetWriter::SCHEMA` and `#typed_rows` to include:
* **Dividend & Corporate Actions:**
  - `declaration_date` (`string`) – Announcement date from `fundamental[:declarationDate]` / quote.
  - `next_div_ex_date` (`string`) – Next ex-dividend date from `fundamental[:nextDividendDate]` / `quote[:nextDivExDate]`.
  - `next_div_pay_date` (`string`) – Next payout date from `fundamental[:nextDividendPayDate]` / `quote[:nextDivPayDate]`.
* **Earnings & Revenue Changes:**
  - `eps_change` (`double`) – `fundamental[:epsChange]`
  - `eps_change_year` (`double`) – `fundamental[:epsChangeYear]`
  - `rev_change_in` (`double`) – `fundamental[:revChangeIn]`
* **Instrument & Borrow / Short Mechanics:**
  - `cusip` (`string`) – Instrument CUSIP identifier from `inst[:cusip]` / `quote[:reference][:cusip]`.
  - `fund_leverage_factor` (`double`) – `fundamental[:fundLeverageFactor]` (e.g. 100.0, 200.0).
  - `is_hard_to_borrow` (`boolean` or `int64`) – `quote[:reference][:isHardToBorrow]`
  - `is_shortable` (`boolean` or `int64`) – `quote[:reference][:isShortable]`
  - `htb_quantity` (`int64`) – `quote[:reference][:htbQuantity]`
  - `htb_rate` (`double`) – `quote[:reference][:htbRate]`
* **Day-End Closing Stats (from `quote[:quote]`):**
  - `close_price` (`double`) – `quote[:closePrice]`
  - `last_price` (`double`) – `quote[:lastPrice]`
  - `net_change` (`double`) – `quote[:netChange]`
  - `net_percent_change` (`double`) – `quote[:netPercentChange]`
  - `total_volume` (`int64`) – `quote[:totalVolume]`

### 2. Job Logic (`FundamentalsJob`)
* In `fetch_quote_supplements`: Extract the additional fields from `quote_data[:fundamental]`, `quote_data[:reference]`, and `quote_data[:quote]` in the existing `get_quotes` response.
* In `build_row`: Merge instrument fields and quote supplements into the complete row hash. Fall back gracefully to `nil` when fields are unavailable.

### 3. DSL Universe Resolution Fix
* Update `Tickrake::DSL::JobBuilder#build_fundamentals_job!` to accept `(config, schedule)`.
* Resolve symbols from `@universe_name`, `@universe_builder`, or `@inline_symbols`.
* Raise an error if no symbols or universe can be resolved for the job.

### 4. Tests & Verification
* Update `spec/fundamentals_job_spec.rb` to mock the expanded response payloads and assert all new columns are correctly written and typed in the generated Parquet output.
* Add DSL specs verifying `build_fundamentals_job!` correctly resolves symbols from named universes (`universe :name`), inline blocks (`universe { ticker ... }`), and `symbols`.

---

## Dependencies & Resources

- `lib/tickrake/fundamentals_job.rb`
- `lib/tickrake/fundamentals_scheduler_runner.rb`
- `lib/tickrake/dsl/fundamentals_builder.rb`
- `lib/tickrake/dsl/job_builder.rb`
- `lib/tickrake/storage/fundamentals_parquet_writer.rb`
- `lib/tickrake/storage/paths.rb`
- `spec/fundamentals_job_spec.rb`
- `spec/dsl/job_builder_spec.rb`
