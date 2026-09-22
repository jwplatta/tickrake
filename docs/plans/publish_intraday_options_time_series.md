# Implementation Plan: Publish Intraday Options Top-of-Series & Time Series History

Downstream consumers (such as charts in `options-monitor` for Greeks evolution, open interest changes, volume build-up, and IV term structure across the trading day) require both:
1. **Top of the series** (latest snapshot) for instant point-in-time state access.
2. **Complete intraday history** (time series of all snapshots taken across the trading day).

In this updated design:
- We build cleanly from first principles (no backwards compatibility baggage).
- `IntradayPublisherJob` manages its own intraday storage lifecycle. `MaintenanceJob` does **not** manage the intraday datastore.
- The DSL adds a `clear_at` parameter (e.g. `clear_at "00:00"`) allowing users to configure the daily eviction/reset time for the intraday store.

---

## Clean Key & Storage Architecture

### 1. S3 / MinIO Intraday Key Structure

- **Intraday History (Time Series)**:
  `intraday/<provider>/options/<sample_date>/<root>_exp<expiration>_<timestamp>.csv`
  - Example: `intraday/schwab/options/2026-09-21/SPXW_exp2026-09-21_2026-09-21_14-30-00.csv`
  - Contains every snapshot taken during the trading day.
  - Active date snapshots are **never evicted** during the trading session.
  - Newly written local snapshots are uploaded incrementally (skipping keys already present in the intraday store).

- **Top of Series (Latest Snapshot)**:
  `intraday/<provider>/options/latest/<root>_exp<expiration>.csv`
  - Example: `intraday/schwab/options/latest/SPXW_exp2026-09-21.csv`
  - Overwritten with the most recent snapshot for that expiration.
  - Eviction: only keys in `intraday/<provider>/options/latest/<root>_exp` that are no longer active expirations are evicted.

### 2. Intraday Root Index (`intraday/<provider>/<root>.json`)

The `option_chains` object has two explicit sections: `latest` and `series`:

```json
{
  "provider": "schwab",
  "root": "SPY",
  "updated_at": "2026-09-21T20:00:00Z",
  "option_chains": {
    "sample_date": "2026-09-21",
    "status": "complete",
    "latest": {
      "sampled_at": "2026-09-21T20:00:00Z",
      "files": [
        {
          "expiration_date": "2026-09-21",
          "format": "csv",
          "uri": "s3://tickrake-intraday/intraday/schwab/options/latest/SPY_exp2026-09-21.csv",
          "row_count": 50
        }
      ]
    },
    "series": [
      {
        "expiration_date": "2026-09-21",
        "sampled_at": "2026-09-21T14:30:00Z",
        "format": "csv",
        "uri": "s3://tickrake-intraday/intraday/schwab/options/2026-09-21/SPY_exp2026-09-21_2026-09-21_14-30-00.csv",
        "row_count": 50
      },
      {
        "expiration_date": "2026-09-21",
        "sampled_at": "2026-09-21T14:35:00Z",
        "format": "csv",
        "uri": "s3://tickrake-intraday/intraday/schwab/options/2026-09-21/SPY_exp2026-09-21_2026-09-21_14-35-00.csv",
        "row_count": 50
      }
    ]
  },
  "candles": { ... }
}
```

---

## Daily Eviction via DSL (`clear_at`)

Users can configure the daily reset time directly in the `intraday_publish` DSL:

```ruby
Tickrake.job "intraday_publisher" do
  schedule do
    every 15.seconds
  end

  intraday_publish do
    datastore :minio_intraday
    clear_at "00:00" # Defaults to midnight ("00:00"). Can be set to any "HH:MM", or nil to disable.
  end
end
```

When `IntradayPublisherJob` runs and the current local time passes `clear_at` for the current date, it clears the intraday store (`intraday/` prefix), giving the new trading day a fresh store while retaining all history until that reset time.

---

## Proposed Changes

### DSL & Configuration

#### [MODIFY] lib/tickrake/dsl/intraday_publish_builder.rb

- Add `clear_at(clock)` method to `IntradayPublishBuilder`.
- Default `@clear_at = "00:00"`.
- Validate format `HH:MM`.
- Output `"clear_at"` in `build!` settings hash.

---

### Core Tracker

#### [MODIFY] lib/tickrake/tracker.rb

- **Add `intraday_series_rows(provider_name:, root:, date: nil)`**:
  Queries `file_metadata_cache` for all option snapshots for the active date (`COALESCE(?, date('now'))`), ordered by `expiration_date, last_observed_at ASC`.
- **Add `date: nil` keyword parameter to `intraday_index_rows` and `intraday_active_roots`** for clean testing and date selection.

```ruby
def intraday_series_rows(provider_name:, root:, date: nil)
  synchronize_db do
    sql = <<~SQL
      SELECT
        f.provider_name,
        f.ticker AS root,
        f.collection_id,
        date(f.last_observed_at) AS sample_date,
        f.last_observed_at AS sampled_at,
        f.expiration_date,
        f.path,
        f.row_count,
        f.file_size,
        f.updated_at
      FROM file_metadata_cache f
      WHERE f.dataset_type = 'options'
        AND f.provider_name = ?
        AND f.ticker = ?
        AND date(f.last_observed_at) = COALESCE(?, date('now'))
      ORDER BY f.expiration_date, f.last_observed_at ASC
    SQL
    db.execute(sql, [provider_name, root, date])
  end
end
```

---

### Intraday Publishing & Eviction

#### [MODIFY] lib/tickrake/intraday_publisher_job.rb

- **Daily Eviction Check (`check_daily_clear(store, now)`)**:
  - If `clear_at` is configured:
    - Compare `now` against `clear_at` (hour and minute).
    - If `now` has reached or passed `clear_at` on the current calendar date and hasn't cleared yet today:
      - List and delete all keys under `intraday/` in the intraday store.
      - Log the daily clearance.
      - Track `@last_cleared_date = now.strftime("%Y-%m-%d")`.
- **Refactor `publish_options(store, datastore_config)`**:
  1. For each active `(provider_name, root)`:
     - Fetch `latest_rows = @runtime.tracker.intraday_index_rows(provider_name: provider_name, root: root)`
     - Fetch `series_rows = @runtime.tracker.intraday_series_rows(provider_name: provider_name, root: root)`
  2. **Publish Time Series Snapshots**:
     - Extract `sample_date = series_rows.first["sample_date"]`
     - Query existing keys in intraday store: `existing_keys = store.list_keys(prefix: "intraday/#{provider_name}/options/#{sample_date}/#{root}_exp")`
     - For each snapshot in `series_rows`:
       - `key = "intraday/#{provider_name}/options/#{sample_date}/#{File.basename(row['path'])}"`
       - Upload if not already present in `existing_keys`.
       - Record in `series` index:
         ```ruby
         {
           "expiration_date" => row["expiration_date"],
           "sampled_at" => row["sampled_at"],
           "format" => "csv",
           "uri" => "s3://#{datastore_config.bucket}/#{key}",
           "row_count" => row["row_count"]
         }
         ```
  3. **Publish Latest Snapshots**:
     - For each row in `latest_rows`:
       - `latest_key = "intraday/#{provider_name}/options/latest/#{root}_exp#{row['expiration_date']}.csv"`
       - `store.upload_file(row["path"], key: latest_key)`
       - Record in `latest_files`:
         ```ruby
         {
           "expiration_date" => row["expiration_date"],
           "format" => "csv",
           "uri" => "s3://#{datastore_config.bucket}/#{latest_key}",
           "row_count" => row["row_count"]
         }
         ```
     - Evict only stale keys in `intraday/#{provider_name}/options/latest/#{root}_exp`:
       `stale_keys = store.list_keys(prefix: "intraday/#{provider_name}/options/latest/#{root}_exp") - uploaded_latest_keys`
       `store.delete_keys(stale_keys) unless stale_keys.empty?`
  4. **Assemble Per-Root Index**:
     - `option_chains`:
       ```ruby
       {
         "sample_date" => sample_date,
         "status" => "complete",
         "latest" => {
           "sampled_at" => latest_rows.first["sampled_at"],
           "files" => latest_files
         },
         "series" => series
       }
       ```

---

### Backlog Tracking

#### [MODIFY] docs/backlog/publish_intraday_options_time_series.md

- Update status to `in-progress` (and `completed` after verification).

---

## Verification Plan

### Automated Tests

1. `spec/dsl/job_builder_spec.rb`:
   - Verify `clear_at` DSL parsing, clock validation, and default `"00:00"`.
2. `spec/tracker_spec.rb`:
   - Verify `intraday_series_rows` returns chronological snapshots for each expiration on the target date.
   - Verify `intraday_index_rows` returns only the top (latest) snapshot per expiration.
3. `spec/intraday_publisher_job_spec.rb`:
   - Verify timestamped time-series snapshots uploaded to `intraday/<provider>/options/<date>/<root>_exp<exp>_<timestamp>.csv`.
   - Verify latest snapshots uploaded to `intraday/<provider>/options/latest/<root>_exp<exp>.csv`.
   - Verify unified root index output matches `{ "sample_date", "status", "latest": { "sampled_at", "files" }, "series": [...] }`.
   - Verify incremental upload skips files already present in the intraday store.
   - Verify daily clear triggers when passing `clear_at` and removes keys under `intraday/`.
   - Verify session does not evict active date's series snapshots.
4. Run test suite:
   ```bash
   bundle exec rspec spec/dsl/job_builder_spec.rb spec/intraday_publisher_job_spec.rb spec/tracker_spec.rb
   bundle exec rspec
   ```
