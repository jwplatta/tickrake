#!/usr/bin/env ruby
# frozen_string_literal: true

# Backfills the file_metadata_cache SQLite table for Schwab option snapshot CSV files
# that are present on disk but missing from the cache.
#
# Usage:
#   bundle exec ruby scripts/backfill_options_metadata_cache.rb [options]
#
# Options:
#   --provider NAME     Only backfill files for this provider (default: all)
#   --ticker TICKER     Only backfill files for this root ticker (e.g. SPXW, AAPL)
#   --dry-run           Print what would be upserted without writing to the database
#   --batch-size N      Number of files to upsert per transaction (default: 500)

require "csv"
require "optparse"
require "time"

require_relative "../lib/tickrake"

FILENAME_PATTERN = /\A(?<root>.+)_exp(?<expiration_date>\d{4}-\d{2}-\d{2})_(?<sample_date>\d{4}-\d{2}-\d{2})_(?<sample_time>\d{2}-\d{2}-\d{2})\.csv\z/.freeze

options = {
  provider: nil,
  ticker: nil,
  dry_run: false,
  batch_size: 500
}

OptionParser.new do |opts|
  opts.banner = "Usage: backfill_options_metadata_cache.rb [options]"
  opts.on("--provider NAME", "Only backfill this provider") { |v| options[:provider] = v }
  opts.on("--ticker TICKER", "Only backfill this root ticker") { |v| options[:ticker] = v.upcase }
  opts.on("--dry-run", "Print upserts without writing") { options[:dry_run] = true }
  opts.on("--batch-size N", Integer, "Upsert batch size (default: 500)") { |v| options[:batch_size] = v }
end.parse!

config = Tickrake::ConfigLoader.load(Tickrake::PathSupport.config_path)
tracker = Tickrake::Tracker.new(config.sqlite_path)

providers = options[:provider] ? [options[:provider]] : Dir.children(config.options_dir).sort

total_found = 0
total_skipped = 0
total_upserted = 0

providers.each do |provider_name|
  provider_dir = File.join(config.options_dir, provider_name)
  next unless Dir.exist?(provider_dir)

  puts "Scanning #{provider_dir} ..."

  batch = []

  Dir.glob(File.join(provider_dir, "**", "*.csv")).sort.each do |path|
    basename = File.basename(path)
    match = FILENAME_PATTERN.match(basename)
    unless match
      $stderr.puts "  SKIP (unrecognized filename): #{path}"
      next
    end

    root = match[:root].upcase
    next if options[:ticker] && root != options[:ticker]

    total_found += 1

    cached = tracker.file_metadata(path)
    stat = File.stat(path)
    if cached && cached["file_mtime"].to_i == stat.mtime.to_i && cached["file_size"].to_i == stat.size
      total_skipped += 1
      next
    end

    sample_time_str = "#{match[:sample_date]} #{match[:sample_time].tr('-', ':')}"
    sampled_at = Time.strptime(sample_time_str, "%Y-%m-%d %H:%M:%S").utc
    observed_at = sampled_at.iso8601

    row_count = [CSV.read(path, headers: true).length, 0].max rescue 0

    attrs = {
      path: path,
      dataset_type: "options",
      provider_name: provider_name,
      ticker: root,
      frequency: nil,
      expiration_date: match[:expiration_date],
      row_count: row_count,
      first_observed_at: observed_at,
      last_observed_at: observed_at,
      file_mtime: stat.mtime.to_i,
      file_size: stat.size,
      updated_at: Time.now
    }

    if options[:dry_run]
      puts "  WOULD UPSERT: #{provider_name}/#{root} exp=#{match[:expiration_date]} sampled_at=#{observed_at} rows=#{row_count}"
      total_upserted += 1
      next
    end

    batch << attrs

    if batch.length >= options[:batch_size]
      tracker.bulk_upsert_file_metadata(batch)
      total_upserted += batch.length
      puts "  Upserted #{total_upserted} entries so far..."
      batch.clear
    end
  end

  unless batch.empty?
    tracker.bulk_upsert_file_metadata(batch)
    total_upserted += batch.length
    batch.clear
  end
end

puts ""
puts "Done."
puts "  Files found:   #{total_found}"
puts "  Already cached: #{total_skipped}"
puts "  Upserted:      #{total_upserted}"
puts "  (dry run — no changes written)" if options[:dry_run]
