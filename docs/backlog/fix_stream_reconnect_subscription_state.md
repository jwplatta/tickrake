---
type: bug
tags: [streaming, schwab, reconnect, market-streams]
title: Re-add active subscriptions after a Schwab stream reconnect
description: Consolidated market streams reconnect without resubscribing after a stale connection, leaving the new socket idle until Schwab closes it.
created: 2026-09-22
updated: 2026-09-23
status: done
priority: high
source: tickrake
---

# Summary

`StreamJob` keeps `@active_subscriptions` after a stale-stream reconnect. Each retry constructs a fresh `SchwabRb::Stream::Client`, but `sync_subscriptions` sees the old names in `@active_subscriptions` and does not call `add` for the new client. The replacement WebSocket has no `SUBS` or `ADD` requests, receives no events, and is closed by Schwab after roughly 30 to 60 seconds. The watchdog then retries with backoff until its attempt limit is reached, and the runner starts the same failing session again.

## Requirements

- When building a replacement stream client, reset or rebuild the active-subscription state before synchronizing the current schedule windows.
- For every in-window subscription, send the appropriate initial `SUBS` request on the new connection and retain its event handler.
- Do not duplicate subscriptions or handlers across reconnects.
- Preserve normal schedule-window `ADD` and `UNSUBS` behavior after the connection is established.
- Emit structured `subscription_add` events after every reconnect for each active logical subscription.

## Tests

- Simulate a stale stream, then verify the next client receives subscriptions for every in-window logical subscription.
- Verify that reconnecting an active futures subscription sends a new `SUBS` request and resumes event handling.
- Verify an out-of-window subscription is not sent during reconnect.
- Verify repeated reconnects do not accumulate duplicate callbacks or subscription requests.

## Dependencies & Resources

- `lib/tickrake/stream_job.rb` — `run_session`, `sync_subscriptions`, and `@active_subscriptions` lifecycle
- `lib/tickrake/stream_runner.rb` — consolidated session lifecycle
- `docs/backlog/fix_stream_jobs.md` — completed watchdog and consolidated-stream work
- Production observation, 2026-09-22: repeated `stream_connect`, `stream_stale`, and remote-host-close events with no `subscription_add` after the first connection
