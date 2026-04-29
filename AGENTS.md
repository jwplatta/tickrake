# Tickrake Agent Guide

This repository is a Ruby gem for scheduled market-data collection. Agents working here should optimize for small, additive changes that preserve existing CLI behavior, storage conventions, and SQLite compatibility.

## Working Rules

- Check the worktree before editing. Do not revert unrelated user changes.
- Prefer targeted edits and focused specs over broad refactors.
- Keep schema changes additive. New SQLite behavior should go through `lib/tickrake/db/migrations/`.
- Preserve on-disk dataset naming conventions unless the task explicitly changes them.
- When touching import, query, or scheduler paths, verify both runtime behavior and metadata-cache behavior.

## Key Paths

- `lib/tickrake/cli.rb`: CLI command parsing and dispatch.
- `lib/tickrake/runtime.rb`: runtime wiring for config, tracker, provider factory, and logger.
- `lib/tickrake/tracker.rb`: SQLite-backed fetch run tracking and file metadata cache.
- `lib/tickrake/importers/`: bulk import flows, including Massive options import.
- `lib/tickrake/query/`: metadata scanners and query formatting.
- `lib/tickrake/storage/`: CSV writers and managed path conventions.
- `lib/tickrake/db/migrations/`: additive SQLite schema migrations.
- `spec/`: RSpec coverage; add or update specs with behavior changes.

## Development Conventions

- Use `bundle exec rspec` for tests. Prefer file-scoped runs while iterating.
- Keep provider-specific logic inside provider or importer classes rather than spreading conditionals across the CLI.
- Prefer explicit, normalized metadata updates when changing file discovery or import flows.
- For concurrency or locking fixes, validate behavior with specs that exercise multiple SQLite connections where practical.

## Storage and Metadata

- Config path: `~/.tickrake/tickrake.yml`
- SQLite metadata DB: `~/.tickrake/tickrake.sqlite3`
- Candle files: `~/.tickrake/data/history/<provider>/<ticker>_<frequency>.csv`
- Option files: `~/.tickrake/data/options/<provider>/<ticker>_exp<date>_<timestamp>.csv`

## Local Developer Overrides

If a developer wants personal agent notes that should not be committed, keep them in an untracked companion file such as `AGENTS.local.md` and exclude it locally with `.git/info/exclude` or a global gitignore rule.

Only the committed `AGENTS.md` should be treated as shared repository policy. Local companion files are useful for personal reminders, but they are not a reliable substitute for shared instructions unless the developer explicitly surfaces them to the agent.
