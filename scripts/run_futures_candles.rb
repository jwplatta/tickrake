#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative "../lib/tickrake"

Tickrake.job "futures_candles" do
  provider :schwab
  type :candles
  symbols "/ES", "/NQ", "/RTY", "/YM"

  schedule do
    at "16:30"
    weekdays
  end

  lookback 90.days

  candles do
    frequencies %w[day 30min 5min 1min]
    start_date "2026-06-01"
  end
end
