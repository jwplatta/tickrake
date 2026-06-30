#!/usr/bin/env ruby
# frozen_string_literal: true

require "optparse"
require "date"
require_relative "../lib/tickrake"

options = {
  config_path: Tickrake::PathSupport.config_path,
  provider_name: nil,
  option_root: nil,
  start_date: nil,
  end_date: nil,
  dry_run: false
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: ruby scripts/delete_compacted_option_sample_csvs.rb --provider NAME --symbol ROOT --start-date YYYY-MM-DD --end-date YYYY-MM-DD [--dry-run] [--config PATH]"

  opts.on("--provider NAME", "Provider folder name for the compacted dataset") { |value| options[:provider_name] = value }
  opts.on("--symbol ROOT", "Option root symbol to inspect (for example SPXW)") { |value| options[:option_root] = value }
  opts.on("--start-date YYYY-MM-DD", "First sample date to inspect") { |value| options[:start_date] = Date.iso8601(value) }
  opts.on("--end-date YYYY-MM-DD", "Last sample date to inspect") { |value| options[:end_date] = Date.iso8601(value) }
  opts.on("--dry-run", "Print the delete plan without deleting local CSV files") { options[:dry_run] = true }
  opts.on("--config PATH", "Path to tickrake config") { |value| options[:config_path] = value }
end

parser.parse!(ARGV)

raise Tickrake::Error, "--provider is required." if options[:provider_name].to_s.empty?
raise Tickrake::Error, "--symbol is required." if options[:option_root].to_s.empty?
raise Tickrake::Error, "--start-date is required." if options[:start_date].nil?
raise Tickrake::Error, "--end-date is required." if options[:end_date].nil?
raise Tickrake::Error, "--end-date must be on or after --start-date." if options[:end_date] < options[:start_date]

config = Tickrake::ConfigLoader.load(options[:config_path])
tracker = Tickrake::Tracker.new(config.sqlite_path)
storage_paths = Tickrake::Storage::Paths.new(config)
archive_service = Tickrake::Storage::S3Archive.new(config)
output = Tickrake::DeleteCompactedOptionSampleCsvOutput.new(stdout: $stdout)

deleted = 0
skipped_missing = 0
skipped_not_uploaded = 0
backfilled_remote_uri = 0
errors = []

(options[:start_date]..options[:end_date]).each do |sample_date|
  csv_path = storage_paths.option_compacted_sample_path(
    provider: options[:provider_name],
    root: options[:option_root],
    sample_date: sample_date,
    format: "csv"
  )

  unless File.exist?(csv_path)
    skipped_missing += 1
    $stdout.puts("Skip #{sample_date.iso8601}: compacted CSV not found at #{csv_path}")
    next
  end

  metadata = tracker.file_metadata(csv_path)
  if metadata.nil?
    errors << "Date #{sample_date.iso8601}: compacted CSV metadata not found: #{csv_path}"
    next
  end

  remote_uri = metadata["remote_uri"].to_s
  if remote_uri.empty?
    begin
      remote_object = archive_service.verify(csv_path)
      local_size = File.size(csv_path)
      if remote_object.size != local_size
        raise Tickrake::Error, "Archived object size mismatch for #{csv_path}: local=#{local_size} remote=#{remote_object.size}"
      end

      remote_uri = remote_object.uri
      if options[:dry_run]
        $stdout.puts("Would backfill remote_uri for #{sample_date.iso8601}: #{remote_uri}")
      else
        tracker.upsert_file_metadata(
          path: csv_path,
          dataset_type: metadata.fetch("dataset_type"),
          provider_name: metadata.fetch("provider_name"),
          ticker: metadata.fetch("ticker"),
          frequency: metadata["frequency"],
          expiration_date: metadata["expiration_date"],
          storage_format: metadata.fetch("storage_format"),
          storage_location: metadata.fetch("storage_location"),
          artifact_status: "ready_local_and_remote",
          remote_uri: remote_uri,
          source_file_count: metadata["source_file_count"],
          row_count: metadata.fetch("row_count"),
          first_observed_at: metadata["first_observed_at"],
          last_observed_at: metadata["last_observed_at"],
          file_mtime: metadata.fetch("file_mtime"),
          file_size: metadata.fetch("file_size"),
          updated_at: Time.now
        )
        $stdout.puts("Backfilled remote_uri for #{sample_date.iso8601}: #{remote_uri}")
      end
      backfilled_remote_uri += 1
    rescue Aws::S3::Errors::ServiceError
      skipped_not_uploaded += 1
      $stdout.puts("Skip #{sample_date.iso8601}: compacted CSV has not been uploaded to S3")
      next
    rescue StandardError => e
      errors << "Date #{sample_date.iso8601}: #{e.message}"
      next
    end
  end

  begin
    result = Tickrake::DeleteCompactedOptionSampleCsv.new(
      config: config,
      tracker: tracker,
      option_root: options[:option_root],
      sample_date: sample_date,
      provider_name: options[:provider_name],
      archive_service: archive_service,
      dry_run: options[:dry_run]
    ).run
    output.emit(result)
    deleted += 1 if result.deleted
  rescue StandardError => e
    errors << "Date #{sample_date.iso8601}: #{e.message}"
  end
end

$stdout.puts("Summary:")
$stdout.puts("  deleted: #{deleted}")
$stdout.puts("  skipped_missing: #{skipped_missing}")
$stdout.puts("  skipped_not_uploaded: #{skipped_not_uploaded}")
$stdout.puts("  backfilled_remote_uri: #{backfilled_remote_uri}")
$stdout.puts("  errors: #{errors.length}")
errors.each { |error| $stderr.puts("  ERROR: #{error}") }

exit(errors.empty? ? 0 : 1)
