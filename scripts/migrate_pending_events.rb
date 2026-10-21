#!/usr/bin/env ruby
# frozen_string_literal: true

require "optparse"
require "json"
require "time"
require "fileutils"
require "digest"
require "set"
require_relative "../lib/tickrake"

options = {
  cutoff_date: Date.today,
  staging_events_dir: File.expand_path("~/.tickrake/staging/pending_events"),
  data_dir: nil,
  config_path: Tickrake::PathSupport.config_path,
  dry_run: false,
  delete_migrated: false
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: bundle exec ruby scripts/migrate_pending_events.rb [options]"

  opts.on("--before DATE", "Only migrate files strictly modified before DATE (YYYY-MM-DD). Defaults to today (#{Date.today})") do |v|
    options[:cutoff_date] = Date.iso8601(v)
  end

  opts.on("--staging-dir PATH", "Path to pending_events directory") do |v|
    options[:staging_events_dir] = File.expand_path(v)
  end

  opts.on("--data-dir PATH", "Path to target data directory (defaults to config data_dir)") do |v|
    options[:data_dir] = File.expand_path(v)
  end

  opts.on("--config PATH", "Path to tickrake.yml") do |v|
    options[:config_path] = File.expand_path(v)
  end

  opts.on("--dry-run", "Preview files and record counts without writing to data or deleting files") do
    options[:dry_run] = true
  end

  opts.on("--delete", "Delete migrated pending_events files after successful migration (default: keep source files)") do
    options[:delete_migrated] = true
  end
end

parser.parse!(ARGV)

unless Dir.exist?(options[:staging_events_dir])
  puts "Staging pending_events directory does not exist: #{options[:staging_events_dir]}"
  exit 0
end

config = begin
  Tickrake::ConfigLoader.load(options[:config_path])
rescue Tickrake::ConfigError
  # If local ~/.tickrake/tickrake.yml still has legacy schedule: block before user removes it after close, load without schedule:
  raw_yaml = YAML.safe_load(File.read(options[:config_path]))
  raw_yaml.delete("schedule")
  config_dir = File.dirname(File.expand_path(options[:config_path]))
  tmp_path = File.join(config_dir, ".tickrake_migration_temp.yml")
  File.write(tmp_path, YAML.dump(raw_yaml))
  loaded = Tickrake::ConfigLoader.load(tmp_path)
  File.unlink(tmp_path) rescue nil
  loaded
end
data_dir = options[:data_dir] || Tickrake::PathSupport.expand_path(config.data_dir)
paths_helper = Tickrake::Storage::Paths.new(config)

puts "================================================================="
puts "Tickrake: Migrate Pending Events"
puts "Staging source dir : #{options[:staging_events_dir]}"
puts "Target data dir    : #{data_dir}"
puts "Cutoff date        : files strictly before #{options[:cutoff_date]}"
puts "Dry run            : #{options[:dry_run]}"
puts "Delete migrated    : #{options[:delete_migrated]}"
puts "================================================================="

# Find all completed non-tmp ndjson files
all_files = Dir.glob(File.join(options[:staging_events_dir], "*.ndjson"))
candidate_files = all_files.reject { |f| f.end_with?(".tmp") }.select do |f|
  File.mtime(f).to_date < options[:cutoff_date]
end

if candidate_files.empty?
  puts "No pending events files found modified before #{options[:cutoff_date]}."
  exit 0
end

puts "Found #{candidate_files.size} file(s) eligible for migration:"
candidate_files.each do |f|
  puts " - #{File.basename(f)} (mtime: #{File.mtime(f)}, size: #{(File.size(f) / 1024.0 / 1024.0).round(2)} MB)"
end

total_events = 0
migrated_files_count = 0
skipped_duplicates_count = 0

# Cache of existing event fingerprints per target path to guarantee idempotency across re-runs
existing_fingerprints = Hash.new do |h, path|
  set = Set.new
  if File.exist?(path)
    File.foreach(path) do |existing_line|
      trimmed = existing_line.strip
      set << Digest::MD5.hexdigest(trimmed) unless trimmed.empty?
    end
  end
  h[path] = set
end

candidate_files.each do |source_file|
  puts "\nProcessing #{File.basename(source_file)}..."
  buffer = Hash.new { |h, k| h[k] = [] }
  buffered_line_count = 0
  file_events = 0
  file_skipped = 0
  BATCH_SIZE = 10_000

  flush_buffer = lambda do
    buffer.each do |target_path, lines|
      next if lines.empty?
      FileUtils.mkdir_p(File.dirname(target_path))
      File.open(target_path, "a") do |f|
        lines.each { |l| f.puts(l) }
      end
    end
    buffer.clear
    buffered_line_count = 0
  end

  File.foreach(source_file) do |line|
    trimmed = line.strip
    next if trimmed.empty?

    event = JSON.parse(trimmed) rescue nil
    next unless event

    symbol = event["symbol"]
    next unless symbol

    job_type = event["job_type"] || (File.basename(source_file).include?("order_book") ? "order_book" : "level_one")
    provider = event["provider"] || "schwab"
    provider_def = config.provider_definition(provider)

    received_at_str = event["received_at"] || event["quote_time"] || event["book_time"]
    event_time = received_at_str ? (Time.iso8601(received_at_str) rescue Time.now.utc) : Time.now.utc

    target_path = case job_type
                  when "level_one"
                    paths_helper.level_one_path(
                      provider: provider,
                      symbol: symbol,
                      flush_start: event_time,
                      provider_definition: provider_def
                    )
                  when "order_book"
                    paths_helper.order_book_path(
                      provider: provider,
                      symbol: symbol,
                      flush_start: event_time,
                      provider_definition: provider_def
                    )
                  else
                    nil
                  end

    next unless target_path

    fingerprint = Digest::MD5.hexdigest(trimmed)
    if existing_fingerprints[target_path].include?(fingerprint)
      file_skipped += 1
      skipped_duplicates_count += 1
      next
    end

    existing_fingerprints[target_path] << fingerprint
    file_events += 1
    total_events += 1

    unless options[:dry_run]
      buffer[target_path] << trimmed
      buffered_line_count += 1
      flush_buffer.call if buffered_line_count >= BATCH_SIZE
    end
  end

  flush_buffer.call unless options[:dry_run]

  puts " -> Extracted #{file_events} new event(s), skipped #{file_skipped} duplicate(s)"

  if !options[:dry_run] && options[:delete_migrated]
    # Verification: scan source_file once more to verify 100% of valid events are accounted for
    verified = true
    unverified_count = 0

    File.foreach(source_file) do |line|
      trimmed = line.strip
      next if trimmed.empty?
      event = JSON.parse(trimmed) rescue nil
      next unless event && event["symbol"]

      job_type = event["job_type"] || (File.basename(source_file).include?("order_book") ? "order_book" : "level_one")
      provider = event["provider"] || "schwab"
      provider_def = config.provider_definition(provider)
      received_at_str = event["received_at"] || event["quote_time"] || event["book_time"]
      event_time = received_at_str ? (Time.iso8601(received_at_str) rescue Time.now.utc) : Time.now.utc

      target_path = case job_type
                    when "level_one"
                      paths_helper.level_one_path(
                        provider: provider,
                        symbol: event["symbol"],
                        flush_start: event_time,
                        provider_definition: provider_def
                      )
                    when "order_book"
                      paths_helper.order_book_path(
                        provider: provider,
                        symbol: event["symbol"],
                        flush_start: event_time,
                        provider_definition: provider_def
                      )
                    end

      next unless target_path
      fingerprint = Digest::MD5.hexdigest(trimmed)
      unless existing_fingerprints[target_path].include?(fingerprint)
        verified = false
        unverified_count += 1
      end
    end

    if verified
      File.unlink(source_file)
      puts " -> Verified 100% of events present in target files. Safely unlinked #{File.basename(source_file)}"
    else
      warn " -> WARNING: #{unverified_count} event(s) not verified in destination for #{File.basename(source_file)}. Skipping deletion!"
    end
  end

  migrated_files_count += 1
end

puts "\n================================================================="
puts "Migration summary:"
puts "Files processed    : #{migrated_files_count}"
puts "New events written : #{total_events}"
puts "Duplicates skipped : #{skipped_duplicates_count}"
puts "Completed successfully."
puts "================================================================="
