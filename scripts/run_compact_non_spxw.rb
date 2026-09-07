#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative "../lib/tickrake"

Tickrake.job "compact_non_spxw_option_samples" do
  provider :schwab
  type :maintenance

  schedule do
    at "15:40"
    weekdays
  end

  maintenance do
    compact :option_samples, universes: %w[stock_option_symbols etf_option_symbols], delete_sources: true
    archive :option_samples, universes: %w[stock_option_symbols etf_option_symbols],
            to: :s3_archive, artifacts: %i[csv parquet],
            retain: { parquet: true }
  end
end
