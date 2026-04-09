# Contributing

## Development setup

1. Install Ruby 3.1+ and Bundler.
2. Make sure the local `schwab_rb` repo exists at `/Users/jplatta/repos/schwab_rb`, or set `SCHWAB_RB_PATH` to another checkout before installing gems.
3. Install dependencies:

```bash
bundle install
```

4. Initialize Tickrake's local config:

```bash
bundle exec exe/tickrake init
```

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
- Include tests for changes to scheduling, config parsing, DTE resolution, or persistence.
- Document any changes to config shape or operational behavior in `README.md`.
