# Lakehouse Parquet Specifications & Tractatus Handoff Architecture

## 1. Architectural Overview & Separation of Concerns

To eliminate duplicate compaction pipelines, schema drift, and discrepancies between local MinIO caches and the AWS S3 lakehouse, the responsibilities between **Tickrake** and **Tractatus** are delineated as follows:

```
┌────────────────────────────────────────────────────────┐
│                   Tickrake (Ruby)                      │
│            Collector, Scraper, & Streamer              │
├────────────────────────────────────────────────────────┤
│ • Schedules API sessions, manages broker auth & limits │
│ • Streams WebSocket events to staged `.ndjson`         │
│ • Captures option snapshots to raw CSVs                │
│ • Pushes raw intraday files to MinIO staging           │
│ • NO heavy Parquet compaction or schema transformation │
└───────────────────────────┬────────────────────────────┘
                            │
                            │ Raw session payloads (CSVs, ndjson)
                            ▼
┌────────────────────────────────────────────────────────┐
│                   MinIO (Staging)                      │
│        Intraday / Staged Raw Session Bucket            │
└───────────────────────────┬────────────────────────────┘
                            │
                            │ Handoff window: 4:00 PM – 5:00 PM CST
                            ▼
┌────────────────────────────────────────────────────────┐
│                  Tractatus (Python)                    │
│           Compactor & Lakehouse Publisher              │
├────────────────────────────────────────────────────────┤
│ • Reads raw session files from MinIO staging           │
│ • Normalizes schemas (UTC timestamptz, nested structs) │
│ • Slices, sorts, and compacts into canonical Parquet   │
│ • Publishes to PyIceberg Catalog (AWS Glue / S3)       │
│ • Mirrors canonical Parquet to MinIO (1:1 local cache) │
└────────────────────────────────────────────────────────┘
```

---

## 2. Session Lifecycle & Handoff Convention

- **Intraday Capture (08:30 – 16:00 CST)**:
  - Tickrake captures option chains, candles, and streaming tick events (`level_one`, `order_book`).
  - Streaming events are continuously appended to staging `.ndjson` files.
  - Option chain samples are written as raw CSV snapshots.
  - Intraday snapshots are synced to MinIO.
- **EOD Handoff Window (16:00 – 17:00 CST / 22:00 – 23:00 UTC)**:
  - North American equity, futures, and index options markets are fully closed.
  - Tickrake finalizes and seals all session `.ndjson` and CSV files for the date.
  - A convention marker or schedule window confirms Tickrake has completed writes for that market date.
- **Tractatus Normalization & Archival Run (17:00 CST onwards)**:
  - Tractatus wakes up, scans the sealed raw session data from MinIO staging.
  - Normalizes, deduplicates, and compacts data into single daily Parquet files per symbol.
  - Commits to the PyIceberg S3 Lakehouse and updates the local MinIO 1:1 cache.

---

## 3. Canonical Parquet Specifications by Dataset

All datasets consolidated by Tractatus must adhere to the following physical layout, sorting, row group, and compression specifications to maximize backtest event-loop performance and PyIceberg query pruning.

### A. Options Snapshot Chains (`options`)

- **File Granularity**: Exactly **1 file per option root per date**:
  `options/<provider>/<YYYY>/<MM>/<DD>/<root>_samples_<YYYY-MM-DD>.parquet`
- **Physical Sort Order**:
  ```sql
  ORDER BY expiration_date ASC, sampled_at ASC, strike ASC, contract_type ASC, symbol ASC
  ```
  - **Pruning Effect**: Isolates each expiration contract into distinct row groups, enabling DuckDB/PyArrow to prune 80–95%+ of row groups when backtesting specific expirations (e.g. 0DTE/1DTE).
  - **Backtest Locality**: Slicing by `sampled_at` next ensures that when a backtest queries the entire chain at datetime $T$, all strikes are contiguous in memory and naturally ordered by strike ($400, 405, 410 \dots$).
- **Row Group Size**: **`50,000` rows**.
- **Compression Codec**: **`ZSTD`** (Compression Level 3).
- **Key Data Types**:
  - `sampled_at`, `quote_at`, `trade_at`: `timestamp[us, 'UTC']`.
  - `strike`: `float64`.
  - `contract_type`, `settlement_type`, `exercise_type`: Dictionary-encoded `string`.

---

### B. Level One Quotes & Trades (`level_one`)

- **File Granularity**: Exactly **1 consolidated file per symbol per date**:
  `level_one/<provider>/<YYYY>/<MM>/<DD>/<symbol>.parquet`
- **Physical Sort Order**:
  ```sql
  ORDER BY received_at ASC, quote_time ASC, trade_time ASC
  ```
  - **Strict Monotonicity**: Guarantees zero re-sorting in Python when running multi-asset priority-queue event loops in backtesters.
  - **Time-Range Pruning**: Enables skipping pre-market or afternoon row groups when simulating specific windows (e.g. market open).
- **Row Group Size**: **`100,000`–`200,000` rows** (accommodates 1M+ ticks/day with 5–15 row groups).
- **Compression Codec**: **`ZSTD`** (Compression Level 3).
- **Schema & Normalization**:
  - `received_at`, `quote_time`, `trade_time`: `timestamp[us, 'UTC']`.
  - Top-level fields: Unpack all 40+ Schwab Level 1 fields (`ask_id`, `bid_id`, `high_price`, `close_price`, `open_price`, `volume`, etc.) into flat typed columns.
  - Residual `extra_json`: Large string column preserving any unrecognized upstream feed fields without data loss.

---

### C. Order Book Level 2 Depth (`order_book`)

- **File Granularity**: Exactly **1 consolidated file per symbol per date**:
  `order_book/<provider>/<YYYY>/<MM>/<DD>/<symbol>.parquet`
- **Physical Sort Order**:
  ```sql
  ORDER BY received_at ASC, book_time ASC
  ```
- **Row Group Size**: **`25,000`–`50,000` rows** (prevents wide depth arrays from exceeding 30MB memory chunks).
- **Compression Codec**: **`ZSTD`** (Compression Level 5) to achieve 5x–8x compression on depth structures.
- **Nested Schema (Eliminating Python JSON Bottleneck)**:
  - Do **NOT** store `bids_json` or `asks_json` strings in the final Parquet file.
  - Store as native nested Arrow structs:
    ```text
    bids: list<struct<price: double, size: int64, num_orders: int64, venues: list<struct<venue: string, size: int64, sequence_id: int64>>>>
    asks: list<struct<price: double, size: int64, num_orders: int64, venues: list<struct<venue: string, size: int64, sequence_id: int64>>>>
    ```
  - `received_at`, `book_time`: `timestamp[us, 'UTC']`.
  - `sample_date`: `date32`.

---

### D. Candles (`candles`)

- **File Granularity**: **1 file per frequency per symbol per year**:
  `candles/<provider>/<frequency>/<year>/<symbol>.parquet`
- **Physical Sort Order**:
  ```sql
  ORDER BY datetime ASC
  ```
- **Row Group Size**: `50,000` rows.
- **Compression Codec**: `ZSTD` (Compression Level 3).
- **Data Types**: `datetime`: `timestamp[us, 'UTC']`, `open`, `high`, `low`, `close`: `float64`, `volume`: `int64`.

---

## 4. Refactoring Roadmap

### Phase 1: Tickrake Simplification
1. **Remove Intermediate Parquet Writers**:
   - Deprecate `LevelOneParquetWriter` and `OrderBookParquetWriter`.
   - Update `EventsIngestorJob` so streaming events remain in staging `.ndjson` without creating fragmented `*_HHMMSSZ.parquet` files.
2. **Standardize Staging & Intraday Output**:
   - Stream events to `~/.tickrake/staging/pending_events/*.ndjson`.
   - Option chains to raw snapshot CSVs.
   - Sync raw session files to the MinIO staging/intraday bucket.
3. **Deprecate Local Ruby Compaction**:
   - Retire `DuckdbOptionCompactedWriter` and candle compaction from Tickrake maintenance once Tractatus is ready to ingest raw staging directly.

### Phase 2: Tractatus Implementation
1. **Direct Raw Ingestion**:
   - Update Tractatus discovery to read raw staged `.ndjson` and snapshot CSVs from MinIO staging.
   - Implement the Parquet specifications defined in Section 3 using PyArrow / DuckDB / PyIceberg.
2. **Dual-Target Publishing**:
   - Write canonical Parquet files to the AWS S3 Lakehouse via PyIceberg.
   - Sync the exact same canonical Parquet files to MinIO to serve as a fast, schema-consistent 1:1 local cache.
