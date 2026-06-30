#!/usr/bin/env ruby
# frozen_string_literal: true

require "optparse"
require "date"
require "thread"
require_relative "../lib/tickrake"

options = {
  config_path: Tickrake::PathSupport.config_path,
  provider_name: nil,
  ticker: nil,
  start_date: nil,
  end_date: nil,
  concurrency: 4,
  dry_run: false
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: ruby scripts/archive_compacted_option_samples.rb --provider NAME --ticker ROOT --start-date YYYY-MM-DD --end-date YYYY-MM-DD [--concurrency N] [--dry-run] [--config PATH]"

  opts.on("--provider NAME", "Provider folder name for the compacted dataset") { |value| options[:provider_name] = value }
  opts.on("--ticker ROOT", "--symbol ROOT", "Option root ticker to archive (for example SPXW)") { |value| options[:ticker] = value }
  opts.on("--start-date YYYY-MM-DD", "First sample date to inspect") { |value| options[:start_date] = Date.iso8601(value) }
  opts.on("--end-date YYYY-MM-DD", "Last sample date to inspect") { |value| options[:end_date] = Date.iso8601(value) }
  opts.on("--concurrency N", Integer, "Number of concurrent archive workers (default: 4)") { |value| options[:concurrency] = value }
  opts.on("--dry-run", "Print the archive plan without uploading or mutating metadata") { options[:dry_run] = true }
  opts.on("--config PATH", "Path to tickrake config") { |value| options[:config_path] = value }
end

parser.parse!(ARGV)

raise Tickrake::Error, "--provider is required." if options[:provider_name].to_s.empty?
raise Tickrake::Error, "--ticker is required." if options[:ticker].to_s.empty?
raise Tickrake::Error, "--start-date is required." if options[:start_date].nil?
raise Tickrake::Error, "--end-date is required." if options[:end_date].nil?
raise Tickrake::Error, "--end-date must be on or after --start-date." if options[:end_date] < options[:start_date]
raise Tickrake::Error, "--concurrency must be positive." if options[:concurrency].to_i <= 0

config = Tickrake::ConfigLoader.load(options[:config_path])
raise Tickrake::Error, "S3 archive is not configured." unless config.s3_archive

queue = Queue.new
(options[:start_date]..options[:end_date]).each { |sample_date| queue << sample_date }

stdout_mutex = Mutex.new
results_mutex = Mutex.new
archived = 0
skipped_missing = 0
errors = []

workers = Array.new(options[:concurrency]) do
  Thread.new do
    tracker = Tickrake::Tracker.new(config.sqlite_path)
    archive_service = Tickrake::Storage::S3Archive.new(config)

    loop do
      sample_date = begin
        queue.pop(true)
      rescue ThreadError
        break
      end

      begin
        result = Tickrake::ArchiveCompactedOptionSamples.new(
          config: config,
          tracker: tracker,
          option_root: options[:ticker],
          sample_date: sample_date,
          provider_name: options[:provider_name],
          archive_service: archive_service,
          dry_run: options[:dry_run]
        ).run

        stdout_mutex.synchronize do
          result.remote_uris.each_value do |uri|
            puts("#{result.dry_run ? 'Would archive' : 'Archived'} #{uri}")
          end
        end
        results_mutex.synchronize { archived += 1 }
      rescue Tickrake::Error => e
        if e.message.start_with?("Compacted artifact not found:")
          stdout_mutex.synchronize do
            puts("Skip #{sample_date.iso8601}: #{e.message}")
          end
          results_mutex.synchronize { skipped_missing += 1 }
        else
          results_mutex.synchronize { errors << "Date #{sample_date.iso8601}: #{e.message}" }
        end
      rescue StandardError => e
        results_mutex.synchronize { errors << "Date #{sample_date.iso8601}: #{e.message}" }
      end
    end
  ensure
    tracker&.close
  end
end

workers.each(&:join)

puts("Summary:")
puts("  archived_dates: #{archived}")
puts("  skipped_missing: #{skipped_missing}")
puts("  errors: #{errors.length}")
errors.each { |error| warn("  ERROR: #{error}") }

exit(errors.empty? ? 0 : 1)
