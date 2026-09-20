#!/usr/bin/env ruby
# frozen_string_literal: true

# Backfill: compact candle CSVs → per-year parquet, archive to S3, write manifests.
#
# Usage:
#   bundle exec ruby scripts/backfill_candles_to_parquet.rb

require_relative "../lib/tickrake"

Tickrake.job "backfill_candles" do
  provider :schwab
  type :maintenance

  maintenance do
    compact :candles
    archive :candles, to: :s3_archive, artifacts: %i[parquet],
            retain: { parquet: true }
  end
end
