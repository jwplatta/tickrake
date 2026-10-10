---
type: bug
tags: [observability, storage, options, parquet, compaction]
title: Correct collection dates and net durable-storage metrics
description: Make structured write events report the option collection date and net bytes added rather than repeatedly reporting full artifact sizes
created: 2026-10-10
updated: 2026-10-10
status: complete
priority: high
source: tickrake
---

# Summary

The byte-count event contract added by PR #103 is sufficient to measure raw
option-chain collection volume, but two fields cannot yet support an accurate
daily durable-storage dashboard. `OptionsJob#fetch_success` assigns
`market_date` from the option expiration date instead of the date the snapshot
was collected. In addition, Parquet events report the full post-write
`byte_count`; summing those events counts an artifact again whenever compaction
or a retry rewrites an existing path. This is especially visible for candle
compaction, which rewrites a yearly Parquet artifact.

Correct the date semantics and add an explicit net-storage metric while
preserving `byte_count` as the finalized artifact size.

## Requirements

1. Set `market_date` on option snapshot `fetch_success` events to the
   collection/sample date in Tickrake's configured market timezone, not the
   option expiration date. Retain `expiration_date` as its own field.
2. Add an additive signed integer field, `storage_byte_delta`, to every
   successful finalized Parquet write event:
   - new artifact: final byte count;
   - replacement artifact: final byte count minus the prior finalized size;
   - a shrink may therefore produce a negative value.
3. Preserve `byte_count` as the absolute final file size. Do not redefine it
   as a delta or remove it from the existing events.
4. Determine the prior size for the final destination before an atomic replace
   or overwrite. Emit neither a successful byte-count event nor a delta if the
   write/rename fails.
5. Apply the same semantics to option-sample compaction, candle compaction,
   event ingestion, economic events, and fundamentals. Retain
   `compaction_output: true` where it already distinguishes compaction from
   direct Parquet writes.
6. Add a stable artifact identity to events when `path` alone is insufficient
   for consumers to distinguish a new artifact from an updated one. Do not
   require Grafana to infer this from mutable directory scans.
7. Add focused specs covering:
   - a collection date that differs from the option expiration date;
   - a new Parquet artifact with `storage_byte_delta == byte_count`;
   - a replacement Parquet artifact with the correct positive, zero, and
     negative deltas;
   - write failure emitting no success/delta event.

## Acceptance Criteria

- A daily options dashboard can group raw snapshot bytes by the actual
  collection day.
- Summing `storage_byte_delta` over a period reports net durable Parquet
  storage added, without inflating totals when a compacted artifact is
  rewritten.
- Summing `byte_count` remains available for a separate "Parquet bytes
  written" throughput metric.
- The raw-option-bytes to durable-storage-delta ratio is meaningful for a
  completed option-compaction day.

## Dependencies & Resources

- PR #103: structured write byte-count emission.
- `lib/tickrake/options_job.rb`: option snapshot date and raw byte event.
- `lib/tickrake/maintenance/option_samples/compactor.rb`: option Parquet
  replacement semantics.
- `lib/tickrake/maintenance/candles/compactor.rb`: yearly Parquet rewrites.
- `lib/tickrake/events_ingestor_job.rb`, `economic_events_job.rb`, and
  `fundamentals_job.rb`: direct Parquet write paths.
- Quant Infra Collection Stats dashboard: intended consumer of collection
  volume, net durable storage added, and compression ratio.
