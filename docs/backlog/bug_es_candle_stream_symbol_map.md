---
type: bug
tags: [candles, streaming, symbol-map]
title: /ES candle stream not using symbol map
description: The candle stream for /ES writes to ES.csv instead of ^ES.csv, indicating the symbol map is not being applied
date: 2026-09-21
updated: 2026-09-21
status: not-started
priority: high
source: claude/tickrake
---

# Summary

The candle stream job for `/ES` appears to bypass the symbol map and write output to `ES.csv` instead of the expected `^ES.csv`. This means data lands at the wrong path and won't be found by anything looking up the canonical symbol.

## Requirements

- Candle stream jobs apply the symbol map before constructing output file paths
- `/ES` writes to `^ES.csv` (or whatever the symbol map dictates)
- Verify other futures symbols are similarly affected and fix consistently

## Dependencies & Resources

- `lib/tickrake/jobs/chart_stream_job.rb` — likely where the symbol map should be applied
- `lib/tickrake/storage/` — path construction logic
