---
type: feature
tags: [bitemporal, research]
title: Bitemporal observation and as-of research contract
description: Preserve upstream quote time and arrival time across storage and transforms, enforce point-in-time as-of cutoffs in DataLoader, and test for lookahead leakage
created: 2026-10-13
updated: 2026-10-13
status: not-started
priority: high
source: antigravity/tickrake
---

# Summary

A fundamental requirement for quantitative research and backtesting is strict point-in-time correctness: reconstructing the exact state of the world known to the system at any historical moment $T$. In market data systems, this requires formalizing **bitemporality**:

1. **World Time (Valid / Event Time)**: When an observation was valid or took place in the external world. For option chains and streaming quotes, this is the upstream exchange/broker **quote time** (`quote_time_ms`) or trade execution time. Contract expiration date is contract specification metadata, *not* the event clock.
2. **System Time (Knowledge / Transaction Time)**: When our system physically received the observation and could have acted upon it (`received_at_ms`).

### Core Architectural Principle

> *"The lake preserves immutable observations and both clocks; transformations carry them forward; the research layer enforces the as-of cutoff and tests for leakage. That’s the contract to demonstrate—not a particular database choice."*

### Current Gaps & Anti-Patterns

- **Missing Upstream Quote Time in Option Chains**: `OptionsJob` records a batch-level `sampled_at` timestamp (derived from scheduler run time or file basename), but does not capture `quote_time_in_long` or `trade_time_in_long` provided directly by Schwab contract objects (`SchwabRb::DataObjects::Option`).
- **In-Loop Timestamping Anti-Pattern**: In `OptionsJob#option_sample_rows`, `fetched_at` was calling `Time.now.utc` inside the loop for every single option contract, creating slightly drifting timestamps across a single snapshot batch instead of threading `run_time`/`timestamp` through.
- **Unreliable System Time Proxies**: Relying on sidecar files (`.meta.json`), file mtimes, or SQLite metadata cache update timestamps as a proxy for system time is an anti-pattern. Sidecar creation and metadata ingestion happen asynchronously in batches at intervals significantly slower than when the data arrived in the collector process. System time must be stamped at immediate ingress.
- **Transform Degradation**: Ingestion and compaction pipelines (e.g., DuckDB options compactor, candle compactor) risk collapsing or discarding system receipt time in favor of valid bar time.
- **Lack of As-Of Cutoff & Leakage Testing**: `DataLoader` currently lacks a first-class `as_of:` query parameter to filter out late-arriving data, and there is no automated test harness verifying the absence of lookahead leakage under out-of-order data or restated values.

## Requirements

### 1. Ingress Capture of Both Clocks (The Lake Layer)
- **World Time**:
  - In `OptionsJob`, extract `quote_time_in_long` (and `trade_time_in_long` when available) from `SchwabRb::DataObjects::Option` and persist it per-contract in `OptionSampleRow`, CSV headers, and Parquet schema.
  - For historical option samples where `quote_time_in_long` was not recorded, backfill World Time with the existing `sampled_at` time (which represents the best available historical estimate).
  - In Streaming (`LevelOneJob`, `OrderBookJob`, `StreamJob`), continue enforcing exchange `quote_time_ms` / `trade_time_ms` / `book_time_ms`.
  - In Economic Events, preserve the official publication timestamp (`event_datetime`).
- **System Time**:
  - In `OptionsJob`, pass the single batch ingress `timestamp` (`run_time`) into `option_sample_rows` rather than repeatedly evaluating `Time.now.utc` inside the contract mapping loop.
  - In all collectors (both polling jobs like `OptionsJob` and streaming jobs like `EventsWriter`), capture `received_at_ms` (milliseconds since epoch UTC) immediately upon payload arrival in memory.
  - For historical option samples, backfill System Time with `sampled_at` (so historical data treats World Time = System Time = `sampled_at`).
  - Do not substitute sidecar write times, file mtimes, or metadata sync timestamps for arrival system time.

### 2. Lossless Transformations (The Pipeline Layer)
- Compaction jobs (`Candles::Compactor`, `OptionSamples::Compactor`, `EventsIngestorJob`) must preserve both clocks in downstream Parquet artifacts.
- When deduplicating or merging records (e.g. candles or option snapshots), compaction algorithms must retain lineage and the earliest/latest system arrival times rather than discarding system time.

### 3. As-Of Point-in-Time Enforcement (The Research Layer)
- Extend `Tickrake::DataLoader` methods (`load_option_chains`, `load_candles`, etc.) to accept an optional `as_of:` cutoff timestamp.
- Research query semantics:
  - An observation is included if and only if `received_at <= as_of`.
  - Data with `quote_time <= as_of` but `received_at > as_of` (e.g., late-arriving market data, delayed feeds, or revised figures) must be strictly excluded.
  - If multiple versions of an observation exist, select the latest version where `received_at <= as_of`.

### 4. Lookahead Leakage Test Suite
- Build automated test specs that explicitly simulate:
  - Late-arriving option snapshots or stream events.
  - Retroactive revisions (e.g. macro data revisions or restated fundamentals).
  - Out-of-order event streams.
- Assert the fundamental invariant:
  $$\text{query}(\text{as\_of} = T) \equiv \text{query executed live at real-world time } T$$

## Acceptance Criteria

- Option chain schema includes explicit `quote_time_ms` (World Time) and `received_at_ms` (System Time) for every contract row.
- Neither compaction nor storage transformations drop or overwrite `received_at_ms`.
- `Tickrake::DataLoader` queries accept `as_of:`, properly filtering records where `received_at_ms > as_of`.
- An automated test suite verifies that late-arriving data does not leak into queries parameterized with an earlier `as_of`.

## Dependencies & Resources

- `lib/tickrake/options_job.rb`: Option snapshot collection and Schwab contract extraction.
- `lib/tickrake/data/option_sample_row.rb`: Option row data structure.
- `lib/tickrake/storage/option_sample_writer.rb`: Option CSV headers.
- `lib/tickrake/storage/duckdb_option_compacted_writer.rb`: DuckDB option compaction schema and export.
- `lib/tickrake/storage/level_one_parquet_writer.rb`: Existing reference implementation for dual timestamps (`quote_time_ms` vs `received_at_ms`).
- `lib/tickrake/data_loader.rb`: Public research query layer.
