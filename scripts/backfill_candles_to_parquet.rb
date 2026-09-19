#!/usr/bin/env ruby
# frozen_string_literal: true

# Backfill script: compact candle CSVs → per-year parquet, archive to S3,
# write manifests, and build reconciler indexes.
#
# Usage:
#   bundle exec ruby scripts/backfill_candles_to_parquet.rb [--provider schwab] [--dry-run]
#
# What it does:
#   1. Globs all candle CSVs under {candles_dir}/{provider}/
#   2. Compacts each into per-year parquet files (merge + dedup)
#   3. Archives parquet files to S3
#   4. Writes per-symbol manifest JSONs
#   5. Builds reconciler indexes (per-symbol + candles.json)
#   6. Truncates CSV to headers-only after successful compaction

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

# Build S3 archive
archive_config = config.s3_archive
unless archive_config
  logger.error("No s3_archive datastore configured. Cannot archive or write manifests.")
  exit 1
end
s3_archive = Tickrake::Storage::S3Archive.new(config)

csv_files = Dir.glob(File.join(provider_dir, "**", "*.csv"))
logger.info("Found #{csv_files.size} candle CSV file(s) for provider=#{provider}")

compact_results = []

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
    logger.info("  [dry-run] would compact + archive #{csv_path}")
    next
  end

  # Compact
  result = Tickrake::Maintenance::Candles::Compactor.new(context: context).run
  unless result.successful?
    logger.error("  COMPACT FAILED: #{result.errors.join(", ")}")
    next
  end
  logger.info("  COMPACTED: #{result.row_count} rows → #{result.years_written.join(", ")} year(s)")

  next if result.years_written.empty?

  # Archive + manifest
  archiver = Tickrake::Maintenance::Candles::Archiver.new(
    context: context, s3_archive: s3_archive
  )
  archive_result = archiver.upload(years: result.years_written)
  if archive_result.successful?
    logger.info("  ARCHIVED: #{archive_result.artifact_results.size} file(s) to S3")
  else
    logger.error("  ARCHIVE FAILED: #{archive_result.errors.join(", ")}")
  end

  compact_results << { symbol: symbol, frequency: frequency }
end

# Build reconciler indexes
unless dry_run || compact_results.empty?
  logger.info("Building reconciler indexes...")

  manifest_writer = Tickrake::Index::AtomicJsonWriter.new
  prefix = "manifests/candles/#{provider}/"
  keys = s3_archive.list_keys(prefix: prefix)

  symbols = []
  keys.each do |key|
    raw = s3_archive.download_content(key)
    manifest = JSON.parse(raw)
    symbol = manifest.fetch("symbol")
    symbols << symbol

    local_path = File.join(candles_dir, provider, "#{symbol}.json")
    manifest_writer.write(local_path, manifest)
    s3_archive.upload(local_path)
    logger.info("  INDEX: #{symbol}.json")
  rescue StandardError => e
    logger.warn("  Failed to process manifest #{key}: #{e.message}")
  end

  symbols = symbols.sort.uniq
  unless symbols.empty?
    candles_index = {
      "provider" => provider,
      "updated_at" => Time.now.utc.iso8601,
      "symbols" => symbols
    }
    index_path = File.join(candles_dir, provider, "candles.json")
    manifest_writer.write(index_path, candles_index)
    s3_archive.upload(index_path)
    logger.info("  INDEX: candles.json (#{symbols.size} symbols)")

    cache_dir = File.join(config.data_dir, "index_cache", provider)
    FileUtils.mkdir_p(cache_dir)
    cache_path = File.join(cache_dir, "candles_cache.json")
    manifest_writer.write(cache_path, { "generated_at" => Time.now.utc.iso8601, "symbols" => symbols })
    logger.info("  CACHE: #{cache_path}")
  end
end

logger.info("Done.")
