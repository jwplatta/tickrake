---
type: bug
tags: [streaming, schwab, level-one]
title: Fix stream jobs in tickrake
description: Level 1 stream jobs silently stall — no exception raised, websocket goes dead, data stops flowing
date: 2026-09-21
status: not-started
priority: high
source: claude/tickrake
---

# Summary

On 2026-09-21, the production `futures_level_one` job started normally and received `/ES` `LEVELONE_FUTURES` events for roughly two minutes. Its Schwab stream then went silent while the Ruby runner and Docker container stayed alive. No exception was raised, so the scheduled runner continued waiting and the active `.ndjson.tmp` file never rotated or reached the events ingestor.

## Requirements

- Record a monotonic `last_event_at` whenever an accepted Level 1 event is written.
- While a session is active, check that timestamp on a short interval. If no event arrives for a configurable timeout (start with 60 seconds for `/ES` during its active session), log a structured `stream_stale` event, stop the client, close/rotate the current events file, and reconnect.
- Bound retries with backoff and emit `reconnect_attempt`, `reconnect_success`, and terminal `session_error` events so the condition is observable.
- Do not use quote timestamps as the liveness clock — use local receipt time, because a provider can replay or delay quote timestamps.
- Fix or pin a patched `schwab_rb` client so a `nil` socket read is treated as a disconnect: close the old connection, log it, and reconnect with backoff. (Upstream `schwab_rb` 1.0.3: `Base#run_receive_loop` returns normally on `nil` from `connection.read`, causing `Base#start` to immediately call `connect` again with no disconnect, log, or backoff. In production this produced repeated `userPreference` requests.)
- Serialize access-token refreshes across containers sharing the Schwab token database. All Tickrake containers share one writable `~/.schwab_rb` token database — concurrent client refreshes are a likely contributor to `LOGIN` denials saying the token was invalid or expired. Reload the persisted token after acquiring a cross-process lock rather than refreshing an independently cached token.

## Preferred streaming topology

Run the equity and futures Level 1 subscriptions through **one** Schwab websocket connection, not separate `equity_level_one` and `futures_level_one` containers. Schwab exposes `LEVELONE_EQUITIES` and `LEVELONE_FUTURES` as separate services, but both can be subscribed on the same streamer connection:

```ruby
stream.on(:level_one_equities, symbols: %w[SPY QQQ IWM], fields: :all) { |event| ... }
stream.on(:level_one_futures, symbols: ["/ES"], fields: :all) { |event| ... }
stream.start_async
```

One stream removes the current cross-container login/reconnect interaction and makes liveness, token refresh, and recovery a single-owner concern. Keep the watchdog and the upstream `nil`-read/reconnect fix — consolidation reduces the failure surface but does not make a dropped websocket detectable by itself.

## Tests

- A fake stream that stops invoking the callback causes a reconnect after the configured timeout.
- Events resume after reconnect and the prior pending file is finalized.
- Normal low-volume intervals below the timeout do not trigger reconnect.
- Shutdown still stops promptly and does not race with a reconnect attempt.

## Dependencies & Resources

- `lib/tickrake/jobs/level_one_job.rb` — `run_session` is where the liveness watchdog needs to be added
- `docs/notes/schwab_stream_api.md`
- Related: `bug_es_candle_stream_symbol_map.md`
