#!/usr/bin/env ruby
# frozen_string_literal: true

# Stream Level 1 quotes for /ES futures via Schwab's LEVELONE_FUTURES service.
#
# Usage:
#   TICKRAKE_CONFIG=~/.tickrake-dev/tickrake-dev.yml bundle exec ruby scripts/stream_es_level_one.rb
#
# Requires SCHWAB_API_KEY and SCHWAB_APP_SECRET env vars (via secrets.env or exported).

require_relative "../lib/tickrake"

config = Tickrake::ConfigLoader.load(
  ENV["TICKRAKE_CONFIG"] || Tickrake::PathSupport.config_path
)

client = Tickrake::ClientFactory.new(config).build
stream = SchwabRb::Stream::Client.new(client)

stream.on(:level_one_futures, symbols: ["/ES"], fields: :all) do |event|
  event["content"]&.each do |entry|
    symbol = entry["key"]
    bid    = entry["1"]
    ask    = entry["2"]
    last   = entry["3"]
    volume = entry["8"]
    mark   = entry["24"]

    parts = ["#{symbol}:"]
    parts << "Last=#{last}"   if last
    parts << "Bid=#{bid}"     if bid
    parts << "Ask=#{ask}"     if ask
    parts << "Vol=#{volume}"  if volume
    parts << "Mark=#{mark}"   if mark
    puts parts.join("  ")
  end
end

puts "Streaming /ES Level 1 futures... Ctrl+C to stop."

trap("INT") do
  puts "\nStopping..."
  stream.stop
  exit
end

stream.start
