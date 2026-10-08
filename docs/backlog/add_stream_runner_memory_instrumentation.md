---
type: feature
tags: [streaming, observability, memory, diagnostics]
title: Add memory instrumentation to the stream runner
description: Emit environment-gated Ruby heap and GC measurements from StreamRunner for correlation with container memory in Grafana
created: 2026-10-08
updated: 2026-10-08
status: not-started
priority: medium
source: tickrake/streaming-observability
---

# Summary

The long-running `market_streams` process shows steadily increasing container
memory while processing streaming events. Existing Docker metrics expose total
container memory but cannot distinguish retained Ruby objects from allocator
high-water behavior or native allocations. Add lightweight, opt-in memory
instrumentation to `Tickrake::StreamRunner` that periodically emits structured
Ruby GC and heap measurements through the existing Tickrake JSON logger. These
events can then be parsed by Loki and compared with container memory in Grafana.

## Requirements

1. Gate the instrumentation behind an explicit environment variable such as
   `TICKRAKE_MEMORY_PROFILE=1`; normal jobs must retain their current behavior
   and logging volume when it is disabled.
2. Sample at a configurable, low-frequency interval with a safe default such
   as five minutes.
3. Emit a structured `memory_profile` log event containing, at minimum:
   - process RSS in bytes when available;
   - `heap_live_slots`, `heap_free_slots`, and `heap_allocated_pages`;
   - `old_objects`;
   - `malloc_increase_bytes` and `oldmalloc_increase_bytes`;
   - total allocated and freed objects;
   - major and minor GC counts.
4. Use the existing Tickrake logger so enabled production jobs write the event
   to their normal `.jsonl` file. Do not introduce a separate metrics server or
   logging destination.
5. Tie the sampler lifecycle to `StreamRunner`: start it once, stop it cleanly
   during runner shutdown, and ensure it cannot accumulate duplicate threads
   across stream session reconnects.
6. Treat missing platform-specific RSS sources gracefully. Instrumentation
   must not interrupt or fail the market-data stream.
7. Add focused specs covering the disabled default, emitted structured fields,
   configurable sampling, and clean shutdown without leaked threads.
8. Document how to enable the instrumentation and provide example Loki queries
   using `| json | event="memory_profile" | unwrap <field>`.
9. Keep full `ObjectSpace` heap dumps and allocation tracing out of the default
   implementation. They carry substantially higher overhead and may expose
   sensitive strings; any future dump support must be separately gated and
   documented.

## Dependencies & Resources

- `lib/tickrake/stream_runner.rb`: owns the long-running consolidated stream
  runner lifecycle.
- `lib/tickrake/logger_factory.rb`: preserves structured hash fields in JSONL
  logs consumed by Promtail and Loki.
- `spec/stream_runner_spec.rb`: expected location for focused runner lifecycle
  and logging coverage.
- Quant-infra's Market Data Streams dashboard already exposes
  `docker_container_memory_usage_bytes`; a follow-up dashboard change can graph
  the new Loki fields beside that container-level measurement.
