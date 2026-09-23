---
type: feature
tags: [intraday, options, options-monitor, publishing, s3]
title: Publish full time series of intraday option chain snapshots alongside latest
description: Refactor intraday publishing to publish both latest (top of series) and full intraday history of option chain snapshots for options-monitor charts
created: 2026-09-21
updated: 2026-09-21
status: completed
priority: high
source: antigravity/tickrake
---

# Summary

Currently, `IntradayPublisherJob` only publishes the latest option chain snapshot for each root/expiration to the intraday store (overwriting or maintaining only a single snapshot file per expiration, e.g. `intraday/<provider>/options/<root>_exp<expiration>.csv`, and evicting older keys).

However, downstream consumers—specifically charts in `options-monitor` (such as intraday Greeks evolution, open interest progression, volume build-up, and implied volatility term structure over the trading day)—require the complete time series of snapshots across the day. We need to refactor intraday publishing so that:
1. A "top of the time series" (latest snapshot) is maintained for fast point-in-time state access.
2. The complete intraday time series of snapshots (timestamped per snapshot run) is also published and indexed, giving charts access to historical intraday progression throughout the session.

## Requirements

- **Time Series Snapshots Storage**:
  - Store individual timestamped snapshot files under a deterministic prefix (e.g. `intraday/<provider>/options/<date>/<root>_exp<expiration>_<timestamp>.csv` or parquet equivalent).
  - Do not evict intraday snapshots belonging to the active trading date during the session.
- **Top of Series / Latest Pointer**:
  - Keep a convenient "latest" reference (e.g., `intraday/<provider>/options/latest/<root>_exp<expiration>.csv` or in the root JSON index) so consumers needing only the current state do not have to parse the entire series.
- **Index Support**:
  - Update `ROOT.json` (and/or an intraday manifest) to list the snapshot time series for each expiration or root, including timestamp, row count, and URI.
- **Backward Compatibility**:
  - Ensure existing consumers expecting the single latest snapshot format continue working or can trivially resolve the latest URI.
- **Clean Retention**:
  - Coordinate with the post-close archival / maintenance job so that old intraday time-series files are safely cleaned up after end-of-day compaction and long-term S3 archiving.

## Dependencies & Resources

- `lib/tickrake/intraday_publisher_job.rb`
- `lib/tickrake/tracker.rb` (`intraday_index_rows`, `intraday_active_roots`)
- `docs/plans/intraday_archival_separation.md`
- `docs/plans/minio_intraday_storage.md`
- Related backlog: `publish_intraday_candles_to_intraday_store.md`
