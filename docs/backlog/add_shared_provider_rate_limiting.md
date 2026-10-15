---
type: feature
tags: [rate-limiting, providers, schwab, postgres]
title: Coordinate provider REST rate limits across jobs
description: Apply a shared, provider-specific REST token bucket across Tickrake jobs, with patient waits, shared 429 cooldowns, and rate-limit visibility.
created: 2026-10-15
updated: 2026-10-15
status: completed
priority: high
source: tickrake
---

# Summary

Tickrake has SQLite and PostgreSQL token-bucket backends keyed by provider, but the current acquisition wait is only about two seconds and rate-limit handling is not coordinated with HTTP 429 responses. Make the configured provider limiter the shared coordination point for every REST request issued by jobs using that provider. Each provider must have independent state and settings while reusing the same limiter logic. Start Schwab REST traffic at 110 requests per 60 seconds, allow an individual request to wait up to 120 seconds for a token, and use 429/`Retry-After` feedback plus visibility to identify and respond to provider throttling.

## Requirements

- Reuse the same limiter implementation for providers while maintaining independent token and cooldown state per provider. Concurrent jobs using the same provider share its limiter through the configured database backend; one provider's traffic or cooldown must not affect another provider.
- Route all applicable provider REST calls through the shared provider limiter, rather than implementing separate per-job limits. Keep streaming traffic outside this task.
- Configure Schwab REST calls for a 110-request-per-60-second token bucket. Keep the initial implementation straightforward: capacity is 110 and refill is 110 tokens per 60 seconds. Document that this permits a burst of accumulated tokens; strict rolling-window enforcement and separate burst configuration are out of scope unless production evidence calls for them.
- When no token is available, wait and retry acquisition for up to 120 seconds. Keep database transactions/row locks short and do not sleep while holding them. Raise a distinct rate-limit wait-timeout error only after the deadline expires, including provider and elapsed wait in the diagnostic.
- On an HTTP 429, apply a provider-wide cooldown so other jobs using that provider wait too. Honor a valid `Retry-After` value; when it is missing or invalid, use a bounded fallback cooldown and avoid unbounded waits.
- Emit structured, low-cardinality visibility for provider request counts, token wait counts and durations, rate-limit wait timeouts, HTTP 429 counts, cooldown duration, and other request failures. Do not include credentials or sensitive request data.
- Add tests for concurrent jobs sharing one provider bucket, isolation between providers, wait and timeout behavior, and shared cooldown handling with and without `Retry-After`.
- Document provider rate-limit configuration and operational behavior.

## Dependencies & Resources

- `lib/tickrake/db/rate_limiter.rb`: backend selection for the common limiter interface.
- `lib/tickrake/db/sqlite_rate_limiter.rb` and `lib/tickrake/db/postgres_rate_limiter.rb`: current provider-keyed token buckets and short retry loop.
- `lib/tickrake/options_job.rb` and `lib/tickrake/fundamentals_job.rb`: existing limiter call sites; inventory other REST job/provider paths to ensure shared coverage.
- `lib/tickrake/config.rb` and provider configuration: current `rate_limit_max_requests` and `rate_limit_interval_seconds` settings.
- `docs/backlog/add_schwab_http_response_diagnostics.md`: related REST request correlation and response-metadata diagnostics, useful for associating 429s with calls.
