#!/usr/bin/env ruby
# frozen_string_literal: true

require "date"
require "fileutils"
require "logger"
require "optparse"
require_relative "../lib/tickrake"

options = {
  config_path: Tickrake::PathSupport.config_path,
  provider_name: nil,
  option_root: nil,
  sample_date: Date.new(2026, 7, 1),
  output_dir: File.expand_path("../tmp/option-compaction-inspect", __dir__)
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: bundle exec ruby scripts/inspect_option_compaction_outputs.rb --provider NAME --root ROOT [--date YYYY-MM-DD] [--output-dir PATH] [--config PATH]"
  opts.on("--provider NAME") { |value| options[:provider_name] = value }
  opts.on("--root ROOT", "--ticker ROOT") { |value| options[:option_root] = value }
  opts.on("--date YYYY-MM-DD") { |value| options[:sample_date] = Date.iso8601(value) }
  opts.on("--output-dir PATH") { |value| options[:output_dir] = value }
  opts.on("--config PATH") { |value| options[:config_path] = value }
end

parser.parse!(ARGV)

raise Tickrake::Error, "--provider is required." if options[:provider_name].to_s.empty?
raise Tickrake::Error, "--root is required." if options[:option_root].to_s.empty?

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

output_dir = File.expand_path(options[:output_dir])
FileUtils.mkdir_p(output_dir)

duckdb_prefix = File.join(output_dir, "duckdb_#{options[:option_root]}_#{options[:sample_date].iso8601}")
ruby_prefix = File.join(output_dir, "ruby_#{options[:option_root]}_#{options[:sample_date].iso8601}")

duckdb_result = Tickrake::Storage::DuckdbOptionCompactedWriter.new.write(
  raw_files: raw_files,
  csv_path: "#{duckdb_prefix}.csv",
  parquet_path: "#{duckdb_prefix}.parquet",
  sampled_at_resolver: context.dataset.method(:sampled_at_for_path)
)

built = context.dataset.build_rows(sample_date: options[:sample_date], raw_files: raw_files)
Tickrake::Storage::OptionCompactedWriter.new.write(
  csv_path: "#{ruby_prefix}.csv",
  parquet_path: "#{ruby_prefix}.parquet",
  headers: built.fetch(:headers),
  rows: built.fetch(:rows)
)

def quote_sql(value)
  "'#{value.gsub("'", "''")}'"
end

def print_section(title)
  puts
  puts "== #{title} =="
end

DuckDB::Database.open do |db|
  db.connect do |con|
    {
      "duckdb" => "#{duckdb_prefix}.parquet",
      "ruby" => "#{ruby_prefix}.parquet"
    }.each do |label, path|
      print_section(label)
      puts "path=#{path}"
      puts "file_size=#{File.size(path)}"

      print_section("#{label} schema")
      con.query("DESCRIBE SELECT * FROM read_parquet(#{quote_sql(path)})").each do |row|
        puts row.inspect
      end

      print_section("#{label} file metadata")
      con.query("SELECT * FROM parquet_file_metadata(#{quote_sql(path)})").each do |row|
        puts row.inspect
      end

      print_section("#{label} row groups")
      con.query(<<~SQL).each do |row|
        SELECT
          row_group_id,
          COUNT(*) AS column_chunks,
          MIN(num_values) AS min_num_values,
          MAX(num_values) AS max_num_values,
          SUM(total_uncompressed_size) AS total_uncompressed_size,
          SUM(total_compressed_size) AS total_compressed_size
        FROM parquet_metadata(#{quote_sql(path)})
        GROUP BY row_group_id
        ORDER BY row_group_id
      SQL
        puts row.inspect
      end

      print_section("#{label} column encodings")
      con.query(<<~SQL).each do |row|
        SELECT
          path_in_schema,
          type,
          stats_min,
          stats_max,
          compression,
          encodings,
          COUNT(*) AS chunk_count
        FROM parquet_metadata(#{quote_sql(path)})
        GROUP BY path_in_schema, type, stats_min, stats_max, compression, encodings
        ORDER BY path_in_schema
      SQL
        puts row.inspect
      end
    end
  end
end

puts
puts "raw_files=#{raw_files.length}"
puts "row_count=#{duckdb_result.row_count}"
puts "output_dir=#{output_dir}"
