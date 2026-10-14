---
type: feature
tags: [options, storage]
title: Ingest missing contract fields and quote times in OptionsJob
description: Add quote_at, trade_at, and valuable pricing fields from Schwab Option objects to option samples
created: 2026-10-13
updated: 2026-10-13
status: complete
priority: high
source: antigravity/tickrake
---

# Summary

Tickrake's `OptionsJob` ingests option chains using `schwab_rb`, but drops several valuable fields present on `SchwabRb::DataObjects::Option`. Most critically for research and bitemporality, it omits the upstream quote timestamp (`quote_time_in_long`) and trade timestamp (`trade_time_in_long`). It also drops useful pricing and contract classification attributes (such as `time_value`, `net_change`, and `settlement_type`).

This task separates the schema additions and collector updates for `OptionsJob` into a standalone, focused feature so that option snapshot CSVs and Parquet artifacts capture complete contract observations at ingress.

## Available Fields & Normalization

Comparing `SchwabRb::DataObjects::Option` to `OptionSampleWriter::CSV_HEADERS`:

### Critical Timing & Temporal Fields (for Bitemporality)
1. **`quote_at`**: Normalized ISO 8601 UTC timestamp converted from broker `quote_time_in_long` (ms) when the bid/ask quote was generated at the exchange/broker (true World Time).
2. **`trade_at`**: Normalized ISO 8601 UTC timestamp converted from broker `trade_time_in_long` (ms) when the option contract last traded (nullable).

### Valuable Analytical & Pricing Fields
3. **`time_value`**: Option extrinsic time premium calculated by the broker.
4. **`net_change`**: Price change from previous close.
5. **`percent_change`**: Percentage price change from previous close.

### Contract Specification & Settlement Fields
6. **`settlement_type`**: Settlement style (e.g., `'AM'`, `'PM'`), essential for index options like SPX vs SPXW.
7. **`exercise_type`**: Exercise style (e.g., `'AMERICAN'`, `'EUROPEAN'`).
8. **`expiration_type`**: Expiration cycle (e.g., `'WEEKLY'`, `'REGULAR'`, `'QUARTERLY'`).
9. **`multiplier`**: Contract multiplier (typically 100).
10. **`non_standard`**: Boolean flag indicating non-standard deliverables or corporate actions.

## Requirements

1. **OptionSampleRow & CSV Header Updates**:
   - Add new attributes to `Tickrake::Data::OptionSampleRow`.
   - Update `Tickrake::Storage::OptionSampleWriter::CSV_HEADERS` additively.
2. **Ingress Extraction in `OptionsJob`**:
   - In `OptionsJob#option_sample_rows`, normalize broker epoch ms to `quote_at` and `trade_at` ISO 8601 UTC strings, extracting `time_value`, `net_change`, `percent_change`, `settlement_type`, `exercise_type`, `expiration_type`, `multiplier`, and `non_standard` from each contract object.
3. **Compaction & Parquet Schemas**:
   - Update `DuckdbOptionCompactedWriter::SCHEMA` and parquet exports to type and carry these new columns forward into compacted Parquet files (`quote_at` and `trade_at` as `TIMESTAMPTZ` / `TIMESTAMP_MICROS`).
   - For historical option samples without `quote_at`, default/backfill `quote_at` with `sampled_at`.
4. **DataLoader Support**:
   - Expose the new fields in `Tickrake::DataLoader#load_option_chains`, parsing `quote_at` and `trade_at` as Ruby `Time` objects.

## Dependencies & Resources

- Related task: `docs/backlog/bitemporal_observation_and_as_of_research_contract.md`
- Gem reference: `schwab_rb/lib/schwab_rb/data_objects/option.rb`
- `lib/tickrake/options_job.rb`: Ingress mapping.
- `lib/tickrake/data/option_sample_row.rb`: In-memory struct.
- `lib/tickrake/storage/option_sample_writer.rb`: CSV headers.
- `lib/tickrake/storage/duckdb_option_compacted_writer.rb`: DuckDB compaction schema.
