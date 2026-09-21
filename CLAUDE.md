# Tickrake — Claude Code Guide

## Workflow

- Follow [CONTRIBUTING.md](CONTRIBUTING.md) for branch naming, commits, changelog, versioning, and PR hygiene.
- Check the worktree before editing. Do not revert unrelated user changes.
- Keep changes targeted. Do not mix unrelated cleanup into the same branch.
- Prefer targeted edits and focused specs over broad refactors.

## Commits & PRs

- Use conventional commit messages (`feat:`, `fix:`, `chore:`, `refactor:`).
- Stage files explicitly by name — never `git add -A` or `git add .`.
- Push small fixes directly to `main`. PRs are optional for this solo project.
- Do not include "Generated with Claude Code" or any attribution footer in PR descriptions.

## Tests

- Write basic unit tests when adding new functionality.
- Tests do not need to be comprehensive, but must cover major code paths and obvious error cases.
- Run `bundle exec rspec` before opening a pull request.

## Project Rules

- Prefer small classes with a single responsibility over large multi-purpose classes.
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

## Storage Paths

- Config: `~/.tickrake/tickrake.yml`
- Metadata DB: `~/.tickrake/tickrake.sqlite3`
- Candles: `~/.tickrake/data/history/<provider>/<ticker>_<frequency>.csv`
- Options: `~/.tickrake/data/options/<provider>/<YYYY>/<MM>/<DD>/<ticker>_exp<date>_<timestamp>.csv`
