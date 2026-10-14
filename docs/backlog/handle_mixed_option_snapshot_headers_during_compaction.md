---
type: bug
tags: [options, storage, compaction]
title: Handle mixed option snapshot headers during compaction
description: Allow same-day option snapshots from before and after additive schema changes to compact together
created: 2026-10-14
updated: 2026-10-14
status: complete
priority: high
source: tickrake
---

# Summary

Option snapshots written before and after the addition of contract fields and quote/trade timestamps can coexist in one day's storage directory. When Tickrake jobs are restarted after a schema deployment, newer snapshots use the expanded CSV header while earlier snapshots retain the old header. The option compaction dataset currently requires exact header equality and aborts when it reaches the first snapshot with a different header. On 2026-10-14, compaction of SPXW stopped at `SPXW_exp2026-10-14_2026-10-14_13-30-27.csv`: the preceding snapshot used the 30-column schema and the failing snapshot used the 40-column schema, adding `quote_at`, `trade_at`, `time_value`, `net_change`, `percent_change`, `settlement_type`, `exercise_type`, `expiration_type`, `multiplier`, and `non_standard`.

Compaction should support compatible additive schema evolution within a day's raw snapshots while preserving all available values and producing the current canonical compacted schema.

## Requirements

- Reproduce compaction with same-day snapshots written using both the legacy and expanded option CSV headers.
- Read each row using its own source header and map values by column name into the current canonical output schema; do not rely on positional `row.fields` alignment across schema versions.
- Populate fields absent from older snapshots as null/empty values, retaining existing historical fallbacks such as deriving `quote_at` from `sampled_at` when it is missing.
- Continue to reject malformed or incompatible inputs, including duplicate headers or unsupported columns, with an error that identifies the file and the specific schema issue.
- Add specs covering mixed legacy/current headers, preservation of new fields from current snapshots, and malformed schema rejection.
- Preserve the existing validation and source-deletion safety: raw snapshots must remain if compaction or validation fails.

## Dependencies & Resources

- Related completed task: `docs/backlog/ingest_missing_option_contract_fields_and_quote_times.md`.
- `lib/tickrake/storage/option_compaction_dataset.rb`.
- `lib/tickrake/storage/option_sample_writer.rb`.
- `lib/tickrake/maintenance/option_samples/validator.rb`.
- `spec/` option compaction dataset and maintenance validation specs.
