#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative "../lib/tickrake"

Tickrake.job "spx_short_dated_options" do
  provider :schwab
  type :options
  universe "spx_symbols"

  schedule do
    every 1.minute
    weekdays from: "08:30", to: "15:05"
  end

  options do
    dte 1..10
  end
end
