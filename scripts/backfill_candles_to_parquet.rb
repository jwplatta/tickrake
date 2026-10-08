#!/usr/bin/env ruby
# frozen_string_literal: true

# Backfill: compact candle CSVs → per-year parquet, archive to S3, write manifests.
#
# Usage:
#   bundle exec ruby scripts/backfill_candles_to_parquet.rb [provider]
#
# Examples:
#   bundle exec ruby scripts/backfill_candles_to_parquet.rb              # defaults to schwab
#   bundle exec ruby scripts/backfill_candles_to_parquet.rb ibkr-paper
#
# Config:
#   Uses TICKRAKE_CONFIG env var, or defaults to ~/.tickrake/tickrake.yml
#   TICKRAKE_CONFIG=config/tickrake.yml bundle exec ruby scripts/backfill_candles_to_parquet.rb

require_relative "../lib/tickrake"

selected_provider = (ARGV[0] || "schwab").to_sym

Tickrake.job "backfill_candles" do
  provider selected_provider
  type :maintenance

  maintenance do
    compact :candles
    archive :candles, to: :s3_archive, artifacts: %i[parquet],
            retain: { parquet: true }
  end
end
