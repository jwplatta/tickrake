# Contributing

## Development setup

1. Install Ruby 3.1+ and Bundler.
2. Install dependencies:

```bash
bundle install
```

3. Initialize Tickrake's local config:

```bash
bundle exec exe/tickrake init
```

## Workflow

1. Start from a focused branch named `feature/...`, `fix/...`, `chore/...`, or `refactor/...`.
2. Keep changes targeted. Do not mix unrelated cleanup into the same branch.
3. Add or update specs for behavior changes.
4. Update the `Unreleased` section in [`CHANGELOG.md`](CHANGELOG.md) for user-visible changes. Skip changelog updates for internal-only refactors, tests, docs, or tooling.
5. Bump `lib/tickrake/version.rb` only when cutting a release, and always pair that bump with a versioned changelog entry.
6. Use semantic versioning for releases: patch for backward-compatible fixes, minor for backward-compatible features, major for breaking changes.
7. Cut a release when `main` has stable user-visible changes worth publishing, or when a user-facing fix should ship immediately.
8. Run relevant checks before opening a pull request.
9. Use conventional commits, for example `feat: add importer resume support` or `fix: refresh metadata after import`.

## Running checks

Run the test suite before opening a pull request:

```bash
bundle exec rspec
```

## Project boundaries

- Keep Tickrake runtime state in `~/.tickrake`.
- Keep market-data payload files in Tickrake-managed directories:
  - `~/.tickrake/data/history`
  - `~/.tickrake/data/options`
- Prefer extending Tickrake for scheduling, orchestration, and tracking concerns.
- Keep broker/API primitives in the underlying client gem when possible.

## Pull requests

- Keep commits focused and intentional.
- Use conventional commit messages.
- Include tests for changes to scheduling, config parsing, DTE resolution, or persistence.
- Document any changes to config shape or operational behavior in `README.md`.
