---
type: feature
tags: [observability, storage, options, streaming, parquet]
title: Emit byte counts for market-data write boundaries
description: Add structured byte-count logging for option snapshots, rotated stream files, and finalized Parquet artifacts
created: 2026-10-10
updated: 2026-10-10
status: complete
priority: high
source: tickrake
---

# Summary

Tickrake currently reports successful collection and persistence with record or
event counts, but it does not consistently emit the number of bytes written at
each durable write boundary. Add structured byte-count events so operations can
measure daily options-chain collection volume, stream-ingress volume, and final
Parquet storage without scanning mutable directories or double-counting
intermediate artifacts.

Option-chain CSV bytes are the primary collection-volume metric. Final Parquet
bytes are the durable-storage metric. They must remain separate measurements;
intermediate writes must not be added to compacted output totals.

## Requirements

1. Emit a structured success event when an option-chain snapshot CSV is
   finalized. Include at least `event`, `data_type`, `byte_count`, `path`,
   `provider`, `symbol`, `market_date`, and the existing row-count or fetch
   identifiers when available.
2. Extend the existing successful stream-file rotation event to include the
   finalized NDJSON file's `byte_count`, path, stream `data_type`, provider,
   symbols, and rotation timestamp. Do not report bytes for an active
   `.ndjson.tmp` file before rotation succeeds.
3. Emit `byte_count` on every finalized Parquet write for option samples,
   candles, and event streams. Include `data_type`, path, provider, applicable
   symbol or symbols, `market_date`, and existing row/event counts.
4. Preserve the write boundary in the event contract. Option-sample compaction
   should identify its Parquet output as compaction output; direct candle or
   event-stream Parquet writes must not be mislabeled as compaction.
5. Obtain byte counts from the finalized local artifact after an atomic write
   succeeds. Failed, partial, retried, or overwritten writes must not emit a
   successful byte-count event.
6. Keep the fields additive and JSON-compatible so Loki/Grafana can aggregate
   `sum_over_time(... | unwrap byte_count [...])` by date, data type, provider,
   or symbol. Do not introduce a directory-scan-based accounting path as the
   source of truth.
7. Add focused specs for option CSV writes, event-file rotation, and each
   Parquet-writing path. Cover finalized byte values and the absence of a
   success event on write failure.

## Acceptance Criteria

- A daily dashboard can separately display option-chain CSV bytes, rotated raw
  event-file bytes, and finalized Parquet bytes in MB/GB.
- The sum of compacted option Parquet bytes is not combined with raw option CSV
  bytes or intermediate artifacts.
- A representative structured event from each write path includes a positive
  integer `byte_count` equal to the finalized artifact size on disk.

## Dependencies & Resources

- `docs/backlog/rotate_staging_files_by_size.md`: completed rotation work that
  already owns stream-file finalization.
- `lib/tickrake/events_writer.rb`: stream staging and rotation boundary.
- `lib/tickrake/events_ingestor_job.rb`: event-stream NDJSON-to-Parquet write
  path.
- Option snapshot writer and DuckDB option-compaction writer: option CSV and
  compacted Parquet boundaries.
- Candle storage writer: candle Parquet boundary, where configured.
- Quant Infra Collection Stats and Data Operations dashboards consume these
  fields through Loki after producer support is available.
