---
type: feature
tags: [s3, index]
title: Extract survivor-free index to S3
description: Extract the survivor-free index from tickrake and publish it to S3 for downstream access
date: 2026-09-21
updated: 2026-09-21
status: not-started
priority: medium
source: claude/tickrake
---

# Summary

The survivor-free index currently lives inside tickrake but is not accessible to external consumers. It needs to be extracted and pushed to S3 so other tools and processes can use it.

## Requirements

- Index is written to a well-known S3 path
- Format is documented so consumers know what to expect
- Process can be run manually and/or scheduled

## Dependencies & Resources

- `docs/architecture.md` — storage path conventions
- `docs/index_publishing.md` — existing index publishing context
