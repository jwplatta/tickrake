#!/usr/bin/env ruby
# frozen_string_literal: true

# Fetches daily historical candles for the COR1M index from IBKR.
# COR1M is the S&P 500 Implied Correlation Index, listed on CBOE.
#
# Usage:
#   bundle exec ruby scripts/ibkr_cor1m_candles.rb
#   bundle exec ruby scripts/ibkr_cor1m_candles.rb --port 7497 --start 2024-01-01 --end 2024-12-31

require "date"
require "time"
require "timeout"
require "optparse"
require "ib-api"

DEFAULTS = {
  host: "127.0.0.1",
  port: 7497,       # paper trading port
  client_id: 1301,
  start_date: Date.today - 30,
  end_date: Date.today,
  bar_size: "1 day",
  timeout: 30
}.freeze

options = DEFAULTS.dup

OptionParser.new do |parser|
  parser.banner = "Usage: bundle exec ruby scripts/ibkr_cor1m_candles.rb [options]"
  parser.on("--host HOST", "TWS/Gateway host (default: #{DEFAULTS[:host]})") { |v| options[:host] = v }
  parser.on("--port PORT", Integer, "TWS/Gateway port (default: #{DEFAULTS[:port]})") { |v| options[:port] = v }
  parser.on("--client-id ID", Integer, "IB client id (default: #{DEFAULTS[:client_id]})") { |v| options[:client_id] = v }
  parser.on("--start DATE", "Start date YYYY-MM-DD (default: #{DEFAULTS[:start_date]})") { |v| options[:start_date] = Date.parse(v) }
  parser.on("--end DATE", "End date YYYY-MM-DD (default: #{DEFAULTS[:end_date]})") { |v| options[:end_date] = Date.parse(v) }
  parser.on("--bar-size SIZE", "Bar size (default: #{DEFAULTS[:bar_size]})") { |v| options[:bar_size] = v }
  parser.on("--timeout SECS", Integer, "Request timeout in seconds (default: #{DEFAULTS[:timeout]})") { |v| options[:timeout] = v }
  parser.on("-h", "--help") { puts parser; exit }
end.parse!

start_date = options[:start_date]
end_date   = options[:end_date]
days = (end_date - start_date).to_i + 1
duration = if days > 365
  years = (days / 365.0).ceil
  "#{years} Y"
else
  "#{days} D"
end
end_dt_str = "#{end_date.strftime("%Y%m%d")} 23:59:59 UTC"

puts "Connecting to #{options[:host]}:#{options[:port]} (client_id=#{options[:client_id]})..."

connection = IB::Connection.new(
  host: options[:host],
  port: options[:port],
  client_id: options[:client_id],
  connect: true,
  received: true
)

contract = IB::Index.new(symbol: "COR1M", exchange: "CBOE", currency: "USD")

puts "Requesting #{options[:bar_size]} bars for COR1M from #{start_date} to #{end_date} (duration: #{duration})..."

request_id = 1
queue = Queue.new

subscription = connection.subscribe(:HistoricalData, :Alert) do |message|
  case message
  when IB::Messages::Incoming::HistoricalData
    queue << message if message.request_id == request_id
  when IB::Messages::Incoming::Alert
    queue << message if message.error_id == request_id
  end
end

connection.send_message(
  IB::Messages::Outgoing::RequestHistoricalData.new(
    request_id: request_id,
    contract: contract,
    end_date_time: end_dt_str,
    duration: duration,
    bar_size: options[:bar_size],
    what_to_show: :trades,
    use_rth: 1,
    keep_up_todate: false
  )
)

message = Timeout.timeout(options[:timeout]) { queue.pop }

if message.is_a?(IB::Messages::Incoming::Alert)
  puts "ERROR: #{message.inspect}"
  exit 1
end

bars = Array(message.results)
puts "Received #{bars.size} bars\n\n"
puts "%-25s %10s %10s %10s %10s %12s" % %w[datetime open high low close volume]
puts "-" * 80
bars.each do |bar|
  dt = bar.time.is_a?(Time) ? bar.time.utc.strftime("%Y-%m-%d %H:%M:%S") : bar.time.to_s
  puts "%-25s %10.4f %10.4f %10.4f %10.4f %12s" % [dt, bar.open, bar.high, bar.low, bar.close, bar.volume]
end

connection.unsubscribe(subscription)
connection.disconnect
