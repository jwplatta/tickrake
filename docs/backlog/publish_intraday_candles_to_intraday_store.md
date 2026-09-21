---
type: feature
tags: [intraday, candles, options-monitor]
title: Publish intraday candles to intraday store
description: Publish intraday candles so the options-monitor can access them via the intraday store
created: 2026-09-21
updated: 2026-09-21
status: not-started
priority: medium
source: claude/tickrake
---

# Summary

Intraday candles are not currently being published to the intraday store, making them inaccessible to the options-monitor. They need to be routed there so the options-monitor can consume them.

## Requirements

- Intraday candles written to the intraday store in the format the options-monitor expects
- Does not break existing candle archival or compaction flows

## Dependencies & Resources

- `docs/plans/intraday_archival_separation.md`
- `docs/plans/minio_intraday_storage.md`
