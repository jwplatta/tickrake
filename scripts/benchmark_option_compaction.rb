#!/usr/bin/env ruby
# frozen_string_literal: true

require "benchmark"
require "date"
require "logger"
require "optparse"
require "tmpdir"
require_relative "../lib/tickrake"

options = {
  config_path: Tickrake::PathSupport.config_path,
  provider_name: nil,
  option_root: nil,
  sample_date: Date.new(2027, 7, 1),
  engine: "duckdb"
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: bundle exec ruby scripts/benchmark_option_compaction.rb --provider NAME --root ROOT [--date YYYY-MM-DD] [--engine duckdb|ruby] [--config PATH]"
  opts.on("--provider NAME") { |value| options[:provider_name] = value }
  opts.on("--root ROOT", "--ticker ROOT") { |value| options[:option_root] = value }
  opts.on("--date YYYY-MM-DD") { |value| options[:sample_date] = Date.iso8601(value) }
  opts.on("--engine NAME") { |value| options[:engine] = value }
  opts.on("--config PATH") { |value| options[:config_path] = value }
end

parser.parse!(ARGV)

raise Tickrake::Error, "--provider is required." if options[:provider_name].to_s.empty?
raise Tickrake::Error, "--root is required." if options[:option_root].to_s.empty?
raise Tickrake::Error, "--engine must be duckdb or ruby." unless %w[duckdb ruby].include?(options[:engine])

config = Tickrake::ConfigLoader.load(options[:config_path])
tracker = Tickrake::Tracker.new(config.sqlite_path)
context = Tickrake::Maintenance::OptionSamples::Context.new(
  config: config,
  tracker: tracker,
  provider_name: options[:provider_name],
  option_root: options[:option_root],
  sample_date: options[:sample_date],
  logger: Logger.new($stderr).tap { |logger| logger.level = Logger::WARN }
)

raw_files = context.dataset.raw_snapshot_files(sample_date: options[:sample_date])
raise Tickrake::Error, "No raw snapshot files found for #{options[:provider_name]} #{options[:option_root]} #{options[:sample_date].iso8601}." if raw_files.empty?

writer = if options[:engine] == "duckdb"
  Tickrake::Storage::DuckdbOptionCompactedWriter.new
else
  Tickrake::Storage::OptionCompactedWriter.new
end

result = nil
elapsed = Benchmark.realtime do
  Dir.mktmpdir("tickrake-option-compaction-benchmark-") do |dir|
    csv_path = File.join(dir, "#{options[:option_root]}_samples_#{options[:sample_date].iso8601}.csv")
    parquet_path = File.join(dir, "#{options[:option_root]}_samples_#{options[:sample_date].iso8601}.parquet")

    result = if options[:engine] == "duckdb"
      writer.write(
        raw_files: raw_files,
        csv_path: csv_path,
        parquet_path: parquet_path,
        sampled_at_resolver: context.dataset.method(:sampled_at_for_path)
      )
    else
      built = context.dataset.build_rows(sample_date: options[:sample_date], raw_files: raw_files)
      writer.write(csv_path: csv_path, parquet_path: parquet_path, headers: built.fetch(:headers), rows: built.fetch(:rows))
      Tickrake::Storage::DuckdbOptionCompactedWriter::Result.new(
        csv_path: csv_path,
        parquet_path: parquet_path,
        row_count: built.fetch(:rows).length,
        first_sampled_at: built.fetch(:sampled_times).min,
        last_sampled_at: built.fetch(:sampled_times).max
      )
    end

    csv_bytes = File.size(result.csv_path)
    parquet_bytes = File.size(result.parquet_path)

    $stdout.puts("engine=#{options[:engine]}")
    $stdout.puts("provider=#{options[:provider_name]}")
    $stdout.puts("root=#{options[:option_root]}")
    $stdout.puts("sample_date=#{options[:sample_date].iso8601}")
    $stdout.puts("raw_files=#{raw_files.length}")
    $stdout.puts("row_count=#{result.row_count}")
    $stdout.puts("first_sampled_at=#{result.first_sampled_at&.utc&.iso8601}")
    $stdout.puts("last_sampled_at=#{result.last_sampled_at&.utc&.iso8601}")
    $stdout.puts("csv_bytes=#{csv_bytes}")
    $stdout.puts("parquet_bytes=#{parquet_bytes}")
  end
end

$stdout.puts(format("elapsed_seconds=%.3f", elapsed))

