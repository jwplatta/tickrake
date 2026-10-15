---
type: feature
tags: [schwab, http, observability, diagnostics]
title: Correlate Schwab REST failures with response metadata
description: Add unique per-request correlation IDs and structured response metadata so malformed or incomplete Schwab responses can be diagnosed across Tickrake jobs.
created: 2026-10-15
updated: 2026-10-15
status: completed
priority: high
source: tickrake
---

# Summary

Recent production option scrapes failed with `JSON::ParserError: unexpected end of input` and request timeouts across multiple jobs. Tickrake records the fetch error, but the shared `schwab_rb` log interleaves requests from multiple processes and uses request counters that are not unique across clients. The current logs therefore cannot reliably associate an HTTP response with a failed fetch or show whether a response body was empty or incomplete.

## Requirements

- Assign every Schwab REST request a correlation ID that is unique across processes and job containers; include the job or service name where available.
- Propagate that ID through request, response, and error logs, and through Tickrake structured `fetch_retry` and `fetch_failed` events.
- Record response status, content type, declared content length, actual response-body byte count, request duration, and parse outcome when available. Preserve response metadata when JSON parsing fails.
- Record timeout and transport failures with the same ID and elapsed time, even when no HTTP response is available.
- Do not log full response bodies, credentials, authorization headers, or unredacted sensitive query parameters.
- Add tests for concurrent requests and malformed, truncated, and empty responses to verify correlation and metadata without leaking response content.

## Dependencies & Resources

- `lib/tickrake/options_job.rb`: emit request correlation IDs and include them in retry and failure events.
- `schwab_rb/lib/schwab_rb/clients/client.rb` and `lib/schwab_rb/clients/base_client.rb`: HTTP response and JSON parsing are owned by the shared Schwab client; coordinate an API that exposes response metadata on parsing and transport failures.
- Keep request IDs distinct across processes; the current `Req 1` style counter is not a globally unique identifier.
