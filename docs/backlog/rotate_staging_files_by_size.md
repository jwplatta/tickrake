---
type: feature
tags: [streaming, events, storage, order_book, level_one]
title: Rotate streaming staging NDJSON files based on file size
description: Support rotating .ndjson.tmp staging files by byte/MB threshold with a fallback maximum time interval
created: 2026-10-01
updated: 2026-10-08
status: complete
priority: medium
source: gemini/tickrake
---

# Summary

Currently, `Tickrake::EventsWriter` rotates active `.ndjson.tmp` event staging files strictly based on elapsed time (`rotation_interval_seconds`). During periods of heavy market activity (e.g. market open, index rebalancing, earnings, or macro data releases), high-throughput feeds like Level One quotes and Order Book updates can generate tens or hundreds of thousands of events per minute. Under a fixed time-based rotation, a single `.ndjson` staging file can grow unpredictably large, leading to high memory spikes when downstream batch ingestion (`EventsIngestorJob`) loads the file into memory and converts it to Parquet.

This feature requests adding size-based rotation to `EventsWriter` (e.g., rotating once a file reaches a configurable size threshold such as 50 MB), while retaining a maximum time interval fallback so low-volume symbols or quiet trading hours do not leave unfinalized `.tmp` files lingering indefinitely without being ingested.

## Requirements

1. **Size-based rotation in `EventsWriter`**:
   - Track file size or accumulated bytes written in `Tickrake::EventsWriter` (e.g. tracking bytes written to `@current_file` or using `@current_file.pos`).
   - Rotate when `current_bytes >= rotation_size_bytes` (or equivalent MB setting).
2. **Hybrid rotation with maximum elapsed time**:
   - Retain a time ceiling (e.g. `max_rotation_interval_seconds` / `rotation_interval_seconds`) so rotation occurs when **either** the size limit is exceeded **or** the maximum time duration has passed.
3. **Configuration & DSL support**:
   - Allow setting `rotation_size_bytes` or `rotation_size_mb` in `order_book` and `stream` DSL blocks / config settings.
   - Provide a sensible default (e.g., 50 MB) or allow purely size-based with standard interval fallbacks.
4. **Stale file recovery adjustment**:
   - Verify that `recover_stale_files` threshold calculation remains robust even if size-based rotation is the primary trigger.
5. **Specs & Documentation**:
   - Add unit tests in `spec/events_writer_spec.rb` verifying rotation triggers immediately upon reaching the size limit.
   - Ensure existing tests pass without regressions.

## Dependencies & Resources

- `lib/tickrake/events_writer.rb`: Active file write and rotation handling.
- `lib/tickrake/order_book_job.rb`: Passes rotation settings to `EventsWriter`.
- `lib/tickrake/stream_job.rb`: Creates `EventsWriter` instances for streaming subscriptions.
- `lib/tickrake/events_ingestor_job.rb`: Downstream batch processor that consumes rotated `.ndjson` files.
- `spec/events_writer_spec.rb`: Spec suite for `EventsWriter`.
