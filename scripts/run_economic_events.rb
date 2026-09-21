#!/usr/bin/env ruby
# frozen_string_literal: true

# One-off: fetch economic events from BLS, FRED, and Alpha Vantage and write parquet files.
#
# Usage:
#   bundle exec ruby scripts/run_economic_events.rb

ENV["TICKRAKE_HOME"]   ||= File.expand_path("~/.tickrake-dev")
ENV["TICKRAKE_CONFIG"] ||= File.expand_path("~/.tickrake-dev/tickrake-dev.yml")

# Load API keys from .env if present
env_file = File.expand_path("../.env", __dir__)
if File.exist?(env_file)
  File.foreach(env_file) do |line|
    line = line.strip
    next if line.empty? || line.start_with?("#")
    key, value = line.split("=", 2)
    ENV[key] ||= value&.gsub(/\A['"]|['"]\z/, "") if key
  end
end

require_relative "../lib/tickrake"

Tickrake.job "economic_events_daily" do
  type :economic_events

  economic_events do
    lookback_days 30
    lookahead_days 90
    categories "economic", "earnings", "fomc"
  end
end
