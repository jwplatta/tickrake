#!/usr/bin/env ruby
# frozen_string_literal: true

# Backfills S3 manifests for option snapshot Parquet files that exist in S3
# but do not yet have a corresponding manifest file under manifests/options/.
#
# Usage:
#   bundle exec ruby scripts/backfill_manifests.rb [options]
#
# Options:
#   --provider NAME   Only process files for this provider (default: all)
#   --dry-run         Print what would be written without uploading anything
#
# Environment:
#   TICKRAKE_CONFIG   Path to tickrake.yml (default: ~/.tickrake/tickrake.yml)

require "json"
require "optparse"
require "time"

require_relative "../lib/tickrake"

# Parquet files are named: <ROOT>_samples_<YYYY-MM-DD>.parquet
# and are stored at: options/<provider>/<ROOT>_samples_<YYYY-MM-DD>.parquet
PARQUET_FILENAME_PATTERN = /\A(?<root>.+)_samples_(?<sample_date>\d{4}-\d{2}-\d{2})\.parquet\z/.freeze

def row_count_for(s3_uri)
  # Attempt to read row count via DuckDB Ruby gem.
  require "duckdb"
  db = DuckDB::Database.open
  conn = db.connect
  result = conn.query("SELECT COUNT(*) FROM read_parquet('#{s3_uri}')")
  result.first[0].to_i
rescue LoadError
  # DuckDB gem not available — return nil and omit from manifest.
  nil
rescue StandardError
  nil
end

options = {
  config_path: ENV.fetch("TICKRAKE_CONFIG", Tickrake::PathSupport.config_path),
  provider: nil,
  dry_run: ARGV.include?("--dry-run")
}

OptionParser.new do |opts|
  opts.banner = "Usage: backfill_manifests.rb [options]"
  opts.on("--provider NAME", "Only process this provider") { |v| options[:provider] = v }
  opts.on("--dry-run", "Print what would be written without uploading") { options[:dry_run] = true }
  opts.on("--config PATH", "Path to tickrake.yml") { |v| options[:config_path] = File.expand_path(v) }
end.parse!

config = Tickrake::ConfigLoader.load(options[:config_path])

unless config.s3_archive
  $stderr.puts "ERROR: s3_archive is not configured in #{options[:config_path]}"
  exit 1
end

s3_archive = Tickrake::Storage::S3Archive.new(config)
manifest_writer = Tickrake::Maintenance::OptionSamples::ManifestWriter.new(s3_archive: s3_archive)

# Determine providers to scan: either the specified one or all under options/ prefix.
providers_to_scan = if options[:provider]
                      [options[:provider]]
                    else
                      all_keys = s3_archive.list_keys(prefix: "options/")
                      all_keys.map { |k| k.split("/")[1] }.compact.uniq.sort
                    end

total_found   = 0
total_skipped = 0
total_written = 0

providers_to_scan.each do |provider|
  prefix = "options/#{provider}/"
  puts "Scanning s3://#{s3_archive.bucket}/#{prefix} ..."

  keys = s3_archive.list_keys(prefix: prefix)
  parquet_keys = keys.select { |k| k.end_with?(".parquet") }

  parquet_keys.each do |key|
    basename = File.basename(key)
    match = PARQUET_FILENAME_PATTERN.match(basename)
    unless match
      $stderr.puts "  SKIP (unrecognized filename): #{key}"
      next
    end

    root        = match[:root]
    sample_date = match[:sample_date]
    total_found += 1

    if manifest_writer.manifest_exists?(dataset_type: "options", provider: provider, root: root, sample_date: sample_date)
      total_skipped += 1
      next
    end

    parquet_uri = "s3://#{s3_archive.bucket}/#{key}"
    csv_key     = key.sub(/\.parquet\z/, ".csv")
    csv_uri     = "s3://#{s3_archive.bucket}/#{csv_key}"
    csv_exists  = s3_archive.object_exists?(csv_key)

    artifacts = []

    row_count = row_count_for(parquet_uri)
    artifacts << {
      "format"    => "parquet",
      "uri"       => parquet_uri,
      "row_count" => row_count
    }.compact

    if csv_exists
      artifacts << {
        "format"    => "csv",
        "uri"       => csv_uri,
        "row_count" => row_count
      }.compact
    end

    if options[:dry_run]
      puts "  WOULD WRITE manifest: provider=#{provider} root=#{root} sample_date=#{sample_date} artifacts=#{artifacts.length}"
      total_written += 1
      next
    end

    manifest_writer.write(
      dataset_type: "options",
      provider:     provider,
      root:         root,
      sample_date:  sample_date,
      artifacts:    artifacts,
      archived_at:  Time.now.utc
    )
    puts "  WROTE manifest: provider=#{provider} root=#{root} sample_date=#{sample_date}"
    total_written += 1
  end
end

puts ""
puts "Done."
puts "  Parquet files found:  #{total_found}"
puts "  Already have manifest: #{total_skipped}"
puts "  Manifests #{options[:dry_run] ? "planned" : "written"}: #{total_written}"
puts "  (dry run — no manifests uploaded)" if options[:dry_run]
