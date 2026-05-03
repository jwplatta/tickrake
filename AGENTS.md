# Tickrake Agent Guide

Applies to Codex and Gemini. Keep changes small, additive, and compatible with existing CLI behavior, storage paths, and SQLite state.

## Workflow

- Check the worktree before editing. Do not revert unrelated user changes.
- Follow [CONTRIBUTING.md](CONTRIBUTING.md) for the expected code-change workflow, branch naming, commits, tests, and PR hygiene.
- If you need shared agent skills, use the `skillex` skill. Pull before using or updating a shared skill, and push updates when the skill itself changes.

## Project Rules

- Prefer targeted edits and focused specs over broad refactors.
- Keep schema changes additive in `lib/tickrake/db/migrations/`.
- Preserve on-disk dataset naming unless the task explicitly changes it.
- When touching import, query, scheduler, or tracker code, verify both runtime behavior and metadata-cache behavior.
- Keep provider-specific behavior inside provider or importer classes, not scattered through the CLI.

## Key Paths

- `lib/tickrake/cli.rb`: CLI parsing and dispatch.
- `lib/tickrake/runtime.rb`: config, tracker, provider factory, logger.
- `lib/tickrake/tracker.rb`: fetch tracking and file metadata cache.
- `lib/tickrake/importers/`: bulk import flows.
- `lib/tickrake/query/`: metadata scanning and query formatting.
- `lib/tickrake/storage/`: CSV writing and path conventions.
- `lib/tickrake/db/migrations/`: additive SQLite migrations.
- `spec/`: RSpec coverage for behavior changes.

## Storage

- Config: `~/.tickrake/tickrake.yml`
- Metadata DB: `~/.tickrake/tickrake.sqlite3`
- Candles: `~/.tickrake/data/history/<provider>/<ticker>_<frequency>.csv`
- Options: `~/.tickrake/data/options/<provider>/<YYYY>/<MM>/<DD>/<ticker>_exp<date>_<timestamp>.csv`

## Local Notes

Keep personal agent notes in an untracked file such as `AGENTS.local.md`. Treat only the committed `AGENTS.md` as shared policy.
