#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative "../lib/tickrake"

Tickrake.job "spx_2dte_options" do
  provider :schwab
  type :options

  universe do
    ticker "$SPX", option_root: "SPXW"
  end

  schedule do
    every 10.seconds
    every_day from: "08:30", to: "16:00"
  end

  options do
    dte 2
  end
end
