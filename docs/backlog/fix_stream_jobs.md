---
type: bug
tags: [streaming]
title: Fix stream jobs in tickrake
description: Stream jobs are broken and need to be fixed
date: 2026-09-21
status: not-started
priority: high
source: claude/tickrake
---

# Summary

Stream jobs in tickrake are not working correctly and need to be diagnosed and fixed. Specific failure modes to be investigated.

## Requirements

- Stream jobs start and run without errors
- Data flows through as expected

## Dependencies & Resources

- `docs/notes/schwab_stream_api.md`
- Related: `bug_es_candle_stream_symbol_map.md`
