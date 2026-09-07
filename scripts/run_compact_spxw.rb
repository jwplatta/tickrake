#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative "../lib/tickrake"

Tickrake.job "compact_spxw" do
  provider :schwab
  type :maintenance

  schedule do
    at "15:30"
    weekdays
  end

  maintenance do
    compact :option_samples, universe: "spx_symbols", delete_sources: true
    archive :option_samples, universe: "spx_symbols",
            to: :s3_archive, artifacts: %i[csv parquet],
            retain: { parquet: true }
  end
end
