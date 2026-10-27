#!/bin/bash
set -e

CONFIG="${TICKRAKE_CONFIG:-$HOME/.tickrake/tickrake.yml}"

if [ ! -f "$CONFIG" ]; then
  echo "Config not found: $CONFIG"
  exit 1
fi

# DSL job file mode: run a Ruby DSL job script in the foreground.
if [ -n "$TICKRAKE_JOB_FILE" ]; then
  if [ ! -f "$TICKRAKE_JOB_FILE" ]; then
    echo "Job file not found: $TICKRAKE_JOB_FILE"
    exit 1
  fi
  exec bundle exec ruby "$TICKRAKE_JOB_FILE"
fi

echo "Error: No job file specified. Set TICKRAKE_JOB_FILE to the path of a DSL job script." >&2
exit 1
