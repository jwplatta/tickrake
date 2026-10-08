---
type: feature
tags: [scheduler, dsl, config]
title: Allow scheduled jobs to run indefinitely without explicit time windows
description: Support interval-scheduled jobs running 24/7 or all day on specified days when from and to time boundaries are omitted
created: 2026-10-08
updated: 2026-10-08
status: not-started
priority: medium
source: antigravity/tickrake
---

# Summary

Currently, interval-scheduled jobs (`every N.seconds / N.minutes`) require explicit `from:` and `to:` time boundaries in their schedule helpers (e.g. `weekdays from: "08:30", to: "15:00"`, `every_day from: "00:00", to: "23:59"`). Calling `weekdays` or `every_day` without arguments sets the day list for daily one-shot jobs (`at "16:00"`), but creates no `SchedulerWindow`.

Because scheduler runners (`OptionsMonitorRunner`, `CandlesSchedulerRunner`, `EventsIngestorRunner`, etc.) check `due?` against `@scheduled_job.windows`, an interval job without explicit windows never matches `in_window?` and does not run. Furthermore, `ConfigLoader` raises an error if an interval job has no windows configured (`At least one candles job window is required`).

We want to allow jobs to run indefinitely without requiring explicit `from:` and `to:` parameters:
1. 24/7 indefinite execution when only `every N` is specified (or `every_day` without `from:` / `to:`).
2. All-day execution on specific days (e.g. `weekdays` with `every N` running throughout the entirety of Monday–Friday from 00:00 to 23:59).

## Requirements

1. **DSL Schedule Builder**:
   - In `Tickrake::DSL::ScheduleBuilder`, allow schedule helpers like `every_day`, `weekdays`, `weekends`, or `days [...]` combined with `every N` without `from:` / `to:` to generate full-day windows (`00:00` to `23:59`) or indicate unbounded execution.
   - If a job defines `every N` with no day/window constraints at all, default to 24/7 indefinite execution across all days.
2. **Scheduler Runner `in_window?` Semantics**:
   - Update runners (`CandlesSchedulerRunner`, `MaintenanceSchedulerRunner`, `EventsIngestorRunner`, etc.) so that an empty `@scheduled_job.windows` for an interval schedule means always in-window (similar to `StreamSubscription#in_window?`), or ensure default windows span `00:00` to `23:59`.
3. **Config Validation**:
   - Update `ConfigLoader` (`validate_candle_schedule!`, `validate_maintenance_schedule!`, etc.) so that interval schedules without explicit windows are permitted when unbounded execution is intended.
4. **Specs & Documentation**:
   - Add unit tests in `spec/dsl/schedule_builder_spec.rb`, `spec/dsl/job_builder_spec.rb`, and scheduler runner specs covering indefinite interval jobs.
   - Update README schedule documentation showing unbounded / 24/7 interval job syntax.

## Dependencies & Resources

- `lib/tickrake/dsl/schedule_builder.rb`
- `lib/tickrake/dsl/job_builder.rb`
- `lib/tickrake/config_loader.rb`
- `lib/tickrake/stream_config.rb` (reference implementation for unbounded windows)
- `spec/dsl/schedule_builder_spec.rb`
- `spec/scheduler_spec.rb`
