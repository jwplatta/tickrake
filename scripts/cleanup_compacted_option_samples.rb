#!/usr/bin/env ruby
# frozen_string_literal: true

require "date"
require "optparse"
require_relative "../lib/tickrake"

TaskResult = Struct.new(:status, :message, keyword_init: true)

def cleanup_sample_date(config:, provider_name:, option_root:, sample_date:, dry_run:)
  tracker = Tickrake::Tracker.new(config.sqlite_path)
  result = Tickrake::CleanupCompactedOptionSamples.new(
    config: config,
    tracker: tracker,
    option_root: option_root,
    sample_date: sample_date,
    provider_name: provider_name,
    dry_run: dry_run
  ).run

  if dry_run
    TaskResult.new(
      status: :planned,
      message: "#{sample_date.iso8601}: would delete #{result.deleted_source_paths.length} raw snapshots and local compacted csv after verifying remote csv/parquet and local parquet"
    )
  else
    TaskResult.new(
      status: :cleaned,
      message: "#{sample_date.iso8601}: deleted #{result.deleted_source_paths.length} raw snapshots, deleted local compacted csv=#{result.deleted_csv}, kept parquet=#{File.basename(result.parquet_path)} locally"
    )
  end
ensure
  tracker&.close
end

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
  opts.banner = "Usage: ruby scripts/cleanup_compacted_option_samples.rb --provider NAME --ticker ROOT --start-date YYYY-MM-DD --end-date YYYY-MM-DD [--concurrency N] [--dry-run] [--config PATH]"

  opts.on("--provider NAME", "Provider folder name for the compacted dataset") { |value| options[:provider_name] = value }
  opts.on("--ticker ROOT", "--symbol ROOT", "Option root ticker to clean up (for example SPXW)") { |value| options[:ticker] = value }
  opts.on("--start-date YYYY-MM-DD", "First sample date to clean up") { |value| options[:start_date] = Date.iso8601(value) }
  opts.on("--end-date YYYY-MM-DD", "Last sample date to clean up") { |value| options[:end_date] = Date.iso8601(value) }
  opts.on("--concurrency N", Integer, "Number of concurrent workers (default: 4)") { |value| options[:concurrency] = value }
  opts.on("--dry-run", "Print the cleanup plan without deleting local files or metadata") { options[:dry_run] = true }
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

dates = (options[:start_date]..options[:end_date]).to_a
queue = Queue.new
dates.each { |sample_date| queue << sample_date }

progress = Tickrake::ProgressReporter.build(total: dates.length, title: "Cleanup", output: $stdout)
message_mutex = Mutex.new
counts_mutex = Mutex.new
counts = Hash.new(0)
errors = []

workers = Array.new(options[:concurrency]) do
  Thread.new do
    loop do
      sample_date = begin
        queue.pop(true)
      rescue ThreadError
        break
      end

      begin
        result = cleanup_sample_date(
          config: config,
          provider_name: options[:provider_name],
          option_root: options[:ticker],
          sample_date: sample_date,
          dry_run: options[:dry_run]
        )
        message_mutex.synchronize { $stdout.puts(result.message) }
        counts_mutex.synchronize { counts[result.status] += 1 }
        progress&.advance(title: "Cleanup #{sample_date.iso8601}")
      rescue StandardError => e
        counts_mutex.synchronize do
          counts[:error] += 1
          errors << "Date #{sample_date.iso8601}: #{e.message}"
        end
        message_mutex.synchronize { warn("ERROR #{sample_date.iso8601}: #{e.message}") }
        progress&.advance(title: "Cleanup #{sample_date.iso8601} failed")
      end
    end
  end
end

workers.each(&:join)
progress&.finish

$stdout.puts("Summary:")
$stdout.puts("  cleaned: #{counts[:cleaned]}")
$stdout.puts("  planned: #{counts[:planned]}")
$stdout.puts("  errors: #{counts[:error]}")

exit(errors.empty? ? 0 : 1)
