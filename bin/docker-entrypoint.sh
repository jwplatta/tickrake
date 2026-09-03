#!/bin/bash
set -e

CONFIG="${TICKRAKE_CONFIG:-$HOME/.tickrake/tickrake.yml}"
TICKRAKE="bundle exec exe/tickrake"

if [ ! -f "$CONFIG" ]; then
  echo "Config not found: $CONFIG"
  exit 1
fi

# Single-job mode: run one job in the foreground (Docker-native — let the
# container runtime handle restarts via restart: unless-stopped).
if [ -n "$TICKRAKE_JOB" ]; then
  exec $TICKRAKE start --job "$TICKRAKE_JOB" --config "$CONFIG"
fi

# Multi-job mode: start all scheduled jobs as supervised background processes.
pids=()

trap 'echo "Shutting down..."; kill "${pids[@]}" 2>/dev/null; wait' SIGTERM SIGINT

while IFS= read -r job_name; do
  echo "Starting job: $job_name"
  $TICKRAKE run --job "$job_name" --supervisor --config "$CONFIG" &
  pids+=($!)
done < <(ruby -ryaml -e "puts YAML.load_file('$CONFIG').fetch('schedule', {}).keys.join('\n')")

echo "All jobs started (${#pids[@]}). Waiting..."
wait
