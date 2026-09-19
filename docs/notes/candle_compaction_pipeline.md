# Candle Compaction Pipeline (PR 2)

## Overview

Compact intraday candle CSVs into per-year parquet files, archive to S3, write manifest JSONs, and extend the reconciler with a candle pass.

## Archive Path

```
{data_dir}/candles/{provider}/{frequency}/{year}/{symbol}.parquet
```

One parquet file per symbol per year. For 1-min candles this is ~90k rows per file (252 trading days × ~390 minutes).

## Compaction Flow

1. Read intraday CSV at `{candles_dir}/{provider}/{frequency}/{symbol}.csv`
2. Determine the year(s) present in the data
3. For each year, merge with existing parquet file (if any) — dedup by datetime
4. Write new parquet file (atomic tmp + rename)
5. Delete or truncate the CSV after successful compaction

This should run as a maintenance task, likely end-of-day after market close.

## S3 Archive

Upload parquet files to S3 at the same path structure:

```
candles/{provider}/{frequency}/{year}/{symbol}.parquet
```

## Manifest JSONs

Path: `manifests/candles/{provider}/{symbol}.json`

Structure:
```json
{
  "provider": "schwab",
  "symbol": "SPY",
  "updated_at": "2026-09-19T...",
  "frequencies": {
    "1min": {
      "years": [
        {
          "year": 2026,
          "row_count": 45000,
          "first_datetime": "2026-01-02T14:30:00Z",
          "last_datetime": "2026-09-19T20:00:00Z",
          "uri": "s3://bucket/candles/schwab/1min/2026/SPY.parquet"
        }
      ]
    }
  }
}
```

## ReconcilerJob Candle Pass

Extend `ReconcilerJob` with `run_candles_pass` (stub already exists):

1. Read all candle manifest JSONs from S3 at `manifests/candles/{provider}/`
2. Group by provider
3. Build a `candles.json` index per provider listing all available symbols, frequencies, and year ranges

## Backfill Strategy

Existing candle CSV files contain full history (not just intraday). One-off backfill scripts needed:

1. Read each CSV, split by year
2. Write per-year parquet files
3. Upload to S3
4. Write manifest JSONs
5. Truncate CSV to keep only today's data (or delete if all data archived)

After backfill, CSVs will only contain intraday data going forward (streaming appends during the day, compacted to parquet end-of-day).
