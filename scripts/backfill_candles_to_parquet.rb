#!/usr/bin/env ruby
# frozen_string_literal: true

# One-off script to compact existing candle CSVs into per-year parquet files.
#
# Usage:
#   bundle exec ruby scripts/backfill_candles_to_parquet.rb [--provider schwab] [--dry-run]
#
# What it does:
#   1. Globs all candle CSVs under {candles_dir}/{provider}/
#   2. For each: reads CSV, splits by year, merges with existing parquet, writes parquet
#   3. Truncates CSV to headers-only after successful compaction

require_relative "../lib/tickrake"

provider = "schwab"
dry_run = false

ARGV.each_with_index do |arg, i|
  case arg
  when "--provider" then provider = ARGV[i + 1]
  when "--dry-run" then dry_run = true
  end
end

config = Tickrake.config
logger = Logger.new($stdout)
logger.formatter = proc { |severity, _time, _prog, msg| "#{severity}: #{msg}\n" }

candles_dir = config.candles_dir
provider_dir = File.join(candles_dir, provider)

unless Dir.exist?(provider_dir)
  logger.error("Provider directory not found: #{provider_dir}")
  exit 1
end

csv_files = Dir.glob(File.join(provider_dir, "**", "*.csv"))
logger.info("Found #{csv_files.size} candle CSV file(s) for provider=#{provider}")

csv_files.each do |csv_path|
  relative = csv_path.delete_prefix("#{provider_dir}/")
  parts = relative.split("/")
  next unless parts.size == 2

  frequency = parts[0]
  symbol = File.basename(parts[1], ".csv")

  context = Tickrake::Maintenance::Candles::Context.new(
    config: config,
    provider_name: provider,
    frequency: frequency,
    symbol: symbol,
    logger: logger
  )

  row_count = File.readlines(csv_path).size - 1
  if row_count <= 0
    logger.info("SKIP #{symbol} #{frequency} (empty CSV)")
    next
  end

  logger.info("COMPACT #{symbol} #{frequency} (#{row_count} rows)")

  if dry_run
    logger.info("  [dry-run] would compact #{csv_path}")
    next
  end

  result = Tickrake::Maintenance::Candles::Compactor.new(context: context).run
  if result.successful?
    logger.info("  OK: #{result.row_count} rows → #{result.years_written.join(", ")} year(s)")
  else
    logger.error("  FAILED: #{result.errors.join(", ")}")
  end
end

logger.info("Done.")
