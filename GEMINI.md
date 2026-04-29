# Tickrake Agent Guide

Applies to Gemini and Codex. Tickrake is a Ruby gem for scheduled candle and options collection with CSV storage and SQLite-backed metadata.

## Workflow

- Check the worktree before editing. Do not revert unrelated user changes.
- Follow [CONTRIBUTING.md](CONTRIBUTING.md) for the expected code-change workflow, branch naming, commits, tests, and PR hygiene.
- Use the `skillex` skill for shared agent skills. Pull before using or updating a shared skill, and push updates when the skill itself changes.

## Project Rules

- Prefer small, targeted edits and focused specs.
- Keep schema changes additive in `lib/tickrake/db/migrations/`.
- Preserve CLI behavior, storage paths, and dataset naming unless the task explicitly changes them.
- When touching import, query, scheduler, or tracker code, verify both runtime behavior and metadata-cache behavior.
- Keep provider-specific behavior inside provider or importer classes.

## Key Paths

- `lib/tickrake/cli.rb`: CLI parsing and dispatch.
- `lib/tickrake/runtime.rb`: config, tracker, provider factory, logger.
- `lib/tickrake/tracker.rb`: fetch tracking and file metadata cache.
- `lib/tickrake/importers/`: bulk import flows.
- `lib/tickrake/query/`: metadata scanning and query formatting.
- `lib/tickrake/storage/`: CSV writing and path conventions.
- `lib/tickrake/db/migrations/`: additive SQLite migrations.
- `spec/`: RSpec coverage for behavior changes.

## Common Commands

- Install deps: `bundle install`
- Init config: `bundle exec exe/tickrake init`
- Validate config: `bundle exec exe/tickrake validate-config`
- Run tests: `bundle exec rspec`
- Run one spec: `bundle exec rspec spec/path/to_spec.rb`

## Storage

- Config: `~/.tickrake/tickrake.yml`
- Metadata DB: `~/.tickrake/tickrake.sqlite3`
- Logs: `~/.tickrake/*.log`
- Candles: `~/.tickrake/data/history/<provider>/<ticker>_<frequency>.csv`
- Options: `~/.tickrake/data/options/<provider>/<ticker>_exp<date>_<timestamp>.csv`
