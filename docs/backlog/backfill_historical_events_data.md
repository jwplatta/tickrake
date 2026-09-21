---
type: chore
tags: [events, backfill]
title: Backfill historical events data
description: Write a script in tickrake to backfill historical economic/calendar events data
date: 2026-09-21
status: not-started
priority: medium
source: claude/tickrake
---

# Summary

Historical events data (economic calendar, earnings, etc.) is not populated for past dates. A backfill script needs to be written directly in tickrake to fetch and store this data retroactively.

## Requirements

- Script lives in `scripts/` in the tickrake repo
- Uses the existing Tickrake job/provider DSL where possible
- Accepts a date range as input
- Idempotent — safe to re-run without duplicating records

## Dependencies & Resources

- `lib/tickrake/jobs/economic_events_job.rb` — existing job to reference
- `docs/notes/economic_events_job_progress.md` — context on current state
