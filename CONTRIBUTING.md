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
4. Run relevant checks before opening a pull request.
5. Use conventional commits, for example `feat: add importer resume support` or `fix: refresh metadata after import`.

## Running checks

Run the test suite before opening a pull request:

```bash
bundle exec rspec
```

## Project boundaries

- Keep Tickrake runtime state in `~/.tickrake`.
- Keep market-data payload files in Schwab's standard directories:
  - `~/.schwab_rb/data/history`
  - `~/.schwab_rb/data/options`
- Prefer extending Tickrake for scheduling, orchestration, and tracking concerns.
- Keep broker/API primitives in the underlying client gem when possible.

## Pull requests

- Keep commits focused and intentional.
- Use conventional commit messages.
- Include tests for changes to scheduling, config parsing, DTE resolution, or persistence.
- Document any changes to config shape or operational behavior in `README.md`.
