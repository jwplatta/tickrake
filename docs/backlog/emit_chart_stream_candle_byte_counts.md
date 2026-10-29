---
type: feature
tags: [observability, storage, streaming, candles]
title: Emit byte counts for streamed candle writes
description: Log structured byte counts for chart-stream candle data appended to local CSV files
created: 2026-10-19
updated: 2026-10-29
status: complete
priority: medium
source: tickrake
---

# Summary

Tickrake's chart stream subscriptions append candle bars to local CSV files, but currently only emit a plain-text flush message. Collection Stats relies on structured byte-count events tagged with `data_type="candles"`, so live streamed candles do not contribute to Raw Written even while the files are growing. Emit a structured event for each successful flush that reports the bytes newly written.

## Requirements

1. After appending a batch of bars succeeds, emit a JSON-compatible structured event with `event`, `data_type: "candles"`, `provider`, `symbol`, `frequency`, `path`, `row_count`, and a positive integer byte field usable by Loki.
2. Report bytes added by the current flush (the file size after the append minus the size before it), rather than the entire mutable CSV size, so summing events over time does not repeatedly count prior data.
3. Emit no successful write event when opening or appending to the CSV fails. Keep the existing human-readable stream log if useful, but ensure the structured event is the source for Collection Stats.
4. Add focused specs covering multiple appends to one file, the reported byte delta, event fields, and write failures.
5. Confirm the event fields work with the Collection Stats Loki query for `data_type="candles"` and do not change chart-stream file paths or CSV contents.

## Acceptance Criteria

- Each successful chart-stream flush produces one structured candle write event with the bytes appended by that flush.
- Collection Stats Raw Written increases as streamed candle CSV data is appended, without re-counting prior bytes in the file.
- Failed writes do not produce a successful byte-count event.

## Dependencies & Resources

- `lib/tickrake/streaming/chart_stream_job.rb`: chart-stream CSV append and current plain-text flush log.
- `spec/streaming/chart_stream_job_spec.rb`: chart-stream behavior coverage.
- `docs/backlog/emit_market_data_write_byte_counts.md`: existing structured byte-count event conventions.
- Quant Infra Collection Stats Candles / Raw Written panel filters structured events for `data_type="candles"` and unwraps `byte_count`.
