---
type: chore
tags: [refactoring, dates, utilities]
title: Extract shared ISO8601 date and time parser helper
description: Extract repeated ISO8601 string parsing (Date vs Time) logic into a centralized helper
created: 2026-10-20
updated: 2026-10-20
status: not-started
priority: medium
source: antigravity/tickrake
---

# Summary

Multiple components across the codebase parse ISO8601 strings into either a `Time` (if timestamp/time components are present) or a `Date` (if a calendar date is provided). For example:

```ruby
if str.include?("T") || str.include?(":")
  Time.iso8601(str)
else
  Date.iso8601(str)
end
```

This pattern is currently duplicated in `lib/tickrake/cli.rb` (`parse_cli_date_option!`), `lib/tickrake/storage/staging_publisher.rb` (`parse_boundary`), and ad-hoc call sites that need flexible date/time boundary handling. Moving this into a shared utility or helper module (e.g. `Tickrake::TimeSupport` or `Tickrake::Core::TimeParser`) will centralize error handling, ISO8601 validation, and parsing semantics.

## Requirements

1. **Centralized Helper**:
   - Provide a shared method (e.g., `Tickrake::TimeSupport.parse_iso8601(str)` or similar) that accepts a string, `Date`, or `Time` object.
   - Parse ISO8601 timestamps containing time indicators (`T` or `:`) into `Time` objects.
   - Parse date-only ISO8601 strings (`YYYY-MM-DD`) into `Date` objects.
   - Return input unmodified if already a `Date` or `Time`.
   - Provide consistent error handling on invalid formats (`ArgumentError` or domain error).
2. **Refactor Call Sites**:
   - Update `lib/tickrake/cli.rb` (`parse_cli_date_option!`) to delegate to the shared helper.
   - Update `lib/tickrake/storage/staging_publisher.rb` (`parse_boundary`) to delegate to the shared helper.
   - Audit and replace any other ad-hoc ISO8601 parsing branches.
3. **Specs**:
   - Add dedicated unit specs testing valid date strings, valid timestamp strings, invalid format error handling, and type passthrough (`Date`/`Time`).

## Dependencies & Resources

- `lib/tickrake/cli.rb`
- `lib/tickrake/storage/staging_publisher.rb`
- `lib/tickrake/core/path_support.rb` (reference for shared helper module conventions in `Tickrake`)
