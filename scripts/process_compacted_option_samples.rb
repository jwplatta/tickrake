#!/usr/bin/env ruby
# frozen_string_literal: true

require "date"
require "logger"
require "optparse"
require_relative "../lib/tickrake"

TaskResult = Struct.new(:status, :message, keyword_init: true)

def process_sample_date(config:, provider_name:, option_root:, sample_date:, dry_run:)
  tracker = Tickrake::Tracker.new(config.sqlite_path)
  processor = Tickrake::Maintenance::OptionSamples::Processor.new(
    config: config,
    tracker: tracker,
    provider_name: provider_name,
    option_root: option_root,
    sample_date: sample_date,
    logger: Logger.new($stderr).tap { |logger| logger.level = Logger::WARN }
  )

  if dry_run
    validation = processor.validate
    if validation.errors.any? && validation.errors != ["Compacted CSV file not found: #{validation.compacted_path}"]
      raise Tickrake::Error, "Validation failed: #{validation.errors.join('; ')}"
    end

    archive = processor.archive(destination_name: "s3_archive", artifacts: %w[csv parquet], retain_local: { "csv" => true, "parquet" => true })
    if archive.errors.empty?
      TaskResult.new(status: :planned, message: "#{sample_date.iso8601}: would archive #{archive.archived_paths.length} artifacts and keep local csv/parquet")
    else
      TaskResult.new(status: :planned, message: "#{sample_date.iso8601}: would compact and archive csv/parquet")
    end
  else
    compact = processor.compact(delete_sources: false)
    raise Tickrake::Error, "Compaction failed: #{compact.errors.join('; ')}" unless compact.successful?

    archive = processor.archive(destination_name: "s3_archive", artifacts: %w[csv parquet], retain_local: { "csv" => true, "parquet" => true })
    raise Tickrake::Error, "Archive failed: #{archive.errors.join('; ')}" unless archive.successful?

    TaskResult.new(status: :archived, message: "#{sample_date.iso8601}: archived #{archive.archived_paths.length} artifacts and kept local csv/parquet")
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
  opts.banner = "Usage: ruby scripts/process_compacted_option_samples.rb --provider NAME --ticker ROOT --start-date YYYY-MM-DD --end-date YYYY-MM-DD [--concurrency N] [--dry-run] [--config PATH]"
  opts.on("--provider NAME") { |value| options[:provider_name] = value }
  opts.on("--ticker ROOT", "--symbol ROOT") { |value| options[:ticker] = value }
  opts.on("--start-date YYYY-MM-DD") { |value| options[:start_date] = Date.iso8601(value) }
  opts.on("--end-date YYYY-MM-DD") { |value| options[:end_date] = Date.iso8601(value) }
  opts.on("--concurrency N", Integer) { |value| options[:concurrency] = value }
  opts.on("--dry-run") { options[:dry_run] = true }
  opts.on("--config PATH") { |value| options[:config_path] = value }
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
progress = Tickrake::ProgressReporter.build(total: dates.length, title: "Process", output: $stdout)
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
        result = process_sample_date(
          config: config,
          provider_name: options[:provider_name],
          option_root: options[:ticker],
          sample_date: sample_date,
          dry_run: options[:dry_run]
        )
        message_mutex.synchronize { $stdout.puts(result.message) }
        counts_mutex.synchronize { counts[result.status] += 1 }
        progress&.advance(title: "Process #{sample_date.iso8601}")
      rescue StandardError => e
        counts_mutex.synchronize do
          counts[:error] += 1
          errors << "Date #{sample_date.iso8601}: #{e.message}"
        end
        message_mutex.synchronize { warn("ERROR #{sample_date.iso8601}: #{e.message}") }
        progress&.advance(title: "Process #{sample_date.iso8601} failed")
      end
    end
  end
end

workers.each(&:join)
progress&.finish
$stdout.puts("Summary:")
$stdout.puts("  archived: #{counts[:archived]}")
$stdout.puts("  planned: #{counts[:planned]}")
$stdout.puts("  errors: #{counts[:error]}")
exit(errors.empty? ? 0 : 1)
