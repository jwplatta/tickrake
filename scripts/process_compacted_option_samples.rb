#!/usr/bin/env ruby
# frozen_string_literal: true

require "date"
require "logger"
require "optparse"
require_relative "../lib/tickrake"

TaskResult = Struct.new(:status, :message, keyword_init: true)

def build_runtime(config:, tracker:, provider_name:)
  Tickrake::Runtime.new(
    config: config,
    tracker: tracker,
    provider_name: provider_name,
    logger: Logger.new($stderr).tap { |logger| logger.level = Logger::WARN }
  )
end

def build_compaction_job(provider_name:, option_root:)
  Tickrake::ScheduledJobConfig.new(
    name: "script_compact_#{provider_name}_#{option_root}",
    type: "maintenance",
    provider: provider_name,
    interval_seconds: nil,
    windows: [],
    run_at: nil,
    days: [],
    lookback_days: nil,
    dte_buckets: [],
    universe: [],
    task: "compact_option_samples",
    settings: { "option_root" => option_root },
    manual: true
  )
end

def process_sample_date(config:, provider_name:, option_root:, sample_date:, dry_run:)
  tracker = Tickrake::Tracker.new(config.sqlite_path)
  storage_paths = Tickrake::Storage::Paths.new(config)
  csv_path = storage_paths.option_compacted_sample_path(
    provider: provider_name,
    root: option_root,
    sample_date: sample_date,
    format: "csv"
  )
  parquet_path = storage_paths.option_compacted_sample_path(
    provider: provider_name,
    root: option_root,
    sample_date: sample_date,
    format: "parquet"
  )
  dataset = Tickrake::Storage::OptionCompactionDataset.new(
    config: config,
    provider_name: provider_name,
    option_root: option_root
  )
  raw_files = dataset.raw_snapshot_files(sample_date: sample_date)
  return TaskResult.new(status: :skipped, message: "#{sample_date.iso8601}: no raw option snapshots found") if raw_files.empty?

  runtime = build_runtime(config: config, tracker: tracker, provider_name: provider_name)
  scheduled_job = build_compaction_job(provider_name: provider_name, option_root: option_root)

  if dry_run
    unless File.exist?(csv_path) && File.exist?(parquet_path)
      return TaskResult.new(
        status: :planned,
        message: "#{sample_date.iso8601}: would compact #{raw_files.length} raw snapshots into #{File.basename(csv_path)} and #{File.basename(parquet_path)}"
      )
    end

    validation = Tickrake::OptionCompactionValidator.new(
      config: config,
      option_root: option_root,
      sample_date: sample_date,
      provider_name: provider_name
    ).validate
    raise Tickrake::Error, "Validation failed: #{validation.errors.join('; ')}" unless validation.safe_to_delete

    archive_result = Tickrake::ArchiveCompactedOptionSamples.new(
      config: config,
      tracker: tracker,
      option_root: option_root,
      sample_date: sample_date,
      provider_name: provider_name,
      dry_run: true
    ).run
    delete_sources_result = Tickrake::DeleteCompactedOptionSamples.new(
      config: config,
      tracker: tracker,
      option_root: option_root,
      sample_date: sample_date,
      provider_name: provider_name,
      dry_run: true
    ).run
    raise Tickrake::Error, "Delete-source validation failed: #{delete_sources_result.errors.join('; ')}" unless delete_sources_result.safe_to_delete

    remote_uri = archive_result.remote_uris.fetch(csv_path)
    TaskResult.new(
      status: :planned,
      message: "#{sample_date.iso8601}: would validate, archive #{archive_result.archived_paths.length} artifacts, delete #{delete_sources_result.source_paths.length} raw snapshots, and leave only #{File.basename(parquet_path)} locally (csv remote=#{remote_uri})"
    )
  else
    Tickrake::MaintenanceTasks::CompactOptionSamples.new(
      runtime: runtime,
      scheduled_job: scheduled_job,
      start_date: sample_date,
      end_date: sample_date
    ).run(now: Time.now)

    validation = Tickrake::OptionCompactionValidator.new(
      config: config,
      option_root: option_root,
      sample_date: sample_date,
      provider_name: provider_name
    ).validate
    raise Tickrake::Error, "Validation failed: #{validation.errors.join('; ')}" unless validation.safe_to_delete

    archive_result = Tickrake::ArchiveCompactedOptionSamples.new(
      config: config,
      tracker: tracker,
      option_root: option_root,
      sample_date: sample_date,
      provider_name: provider_name
    ).run

    delete_sources_result = Tickrake::DeleteCompactedOptionSamples.new(
      config: config,
      tracker: tracker,
      option_root: option_root,
      sample_date: sample_date,
      provider_name: provider_name
    ).run
    raise Tickrake::Error, "Delete-source validation failed: #{delete_sources_result.errors.join('; ')}" unless delete_sources_result.safe_to_delete
    unless delete_sources_result.deletion_errors.empty?
      raise Tickrake::Error, "Delete-source errors: #{delete_sources_result.deletion_errors.join('; ')}"
    end

    delete_csv_result = Tickrake::DeleteCompactedOptionSampleCsv.new(
      config: config,
      tracker: tracker,
      option_root: option_root,
      sample_date: sample_date,
      provider_name: provider_name
    ).run

    remote_uri = archive_result.remote_uris.fetch(csv_path)
    TaskResult.new(
      status: :archived,
      message: "#{sample_date.iso8601}: archived #{archive_result.archived_paths.length} artifacts, deleted #{delete_sources_result.deleted_paths.length} raw snapshots, deleted local compacted csv=#{delete_csv_result.deleted}, kept parquet=#{File.basename(parquet_path)} locally, csv remote=#{remote_uri}"
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
  opts.banner = "Usage: ruby scripts/process_compacted_option_samples.rb --provider NAME --ticker ROOT --start-date YYYY-MM-DD --end-date YYYY-MM-DD [--concurrency N] [--dry-run] [--config PATH]"

  opts.on("--provider NAME", "Provider folder name for the compacted dataset") { |value| options[:provider_name] = value }
  opts.on("--ticker ROOT", "--symbol ROOT", "Option root ticker to process (for example SPXW)") { |value| options[:ticker] = value }
  opts.on("--start-date YYYY-MM-DD", "First sample date to process") { |value| options[:start_date] = Date.iso8601(value) }
  opts.on("--end-date YYYY-MM-DD", "Last sample date to process") { |value| options[:end_date] = Date.iso8601(value) }
  opts.on("--concurrency N", Integer, "Number of concurrent workers (default: 4)") { |value| options[:concurrency] = value }
  opts.on("--dry-run", "Print the plan without mutating local files, metadata, or S3") { options[:dry_run] = true }
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
$stdout.puts("  skipped: #{counts[:skipped]}")
$stdout.puts("  errors: #{counts[:error]}")

exit(errors.empty? ? 0 : 1)
