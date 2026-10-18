#!/usr/bin/env ruby
# frozen_string_literal: true

require "bundler/setup"
require "duckdb"
require "optparse"
require "pathname"

options = {
  data_dir: nil,
  provider: nil,
  root: nil,
  date: nil,
  dry_run: false,
  codec: "ZSTD",
  row_group_size: 50_000
}

OptionParser.new do |opts|
  opts.banner = "Usage: ruby scripts/rewrite_options_parquet.rb [options]"

  opts.on("-d", "--data-dir DIR", "Path to options data directory (default: ~/.tickrake/data/options)") do |v|
    options[:data_dir] = v
  end

  opts.on("-p", "--provider PROVIDER", "Filter by provider (e.g. schwab, massive)") do |v|
    options[:provider] = v
  end

  opts.on("-r", "--root ROOT", "Filter by option root (e.g. QQQ, SPY, SPXW)") do |v|
    options[:root] = v
  end

  opts.on("--date DATE", "Filter by date (YYYY-MM-DD)") do |v|
    options[:date] = v
  end

  opts.on("--codec CODEC", "Parquet compression codec (default: ZSTD)") do |v|
    options[:codec] = v.upcase
  end

  opts.on("--row-group-size N", Integer, "Row group size (default: 50000)") do |v|
    options[:row_group_size] = v
  end

  opts.on("-n", "--dry-run", "Inspect files and print plans without writing") do
    options[:dry_run] = true
  end

  opts.on("-h", "--help", "Show this message") do
    puts opts
    exit 0
  end
end.parse!

base_dir = options[:data_dir] ? File.expand_path(options[:data_dir]) : File.expand_path("~/.tickrake/data/options")

unless Dir.exist?(base_dir)
  puts "Options data directory not found: #{base_dir}"
  exit 0
end

pattern = File.join(base_dir, "**", "*.parquet")
all_files = Dir.glob(pattern).sort

matched_files = all_files.select do |path|
  next false if options[:provider] && !path.include?("/#{options[:provider]}/")
  next false if options[:root] && !File.basename(path).start_with?("#{options[:root]}_")

  if options[:date]
    date_parts = options[:date].split("-") # [YYYY, MM, DD]
    next false unless path.include?("/#{date_parts[0]}/#{date_parts[1]}/#{date_parts[2]}/") || path.include?("_#{options[:date]}.parquet")
  end

  true
end

puts "Found #{matched_files.size} matching file(s) under #{base_dir}"
puts "Settings: CODEC=#{options[:codec]}, ROW_GROUP_SIZE=#{options[:row_group_size]}, DRY_RUN=#{options[:dry_run]}"
puts "-" * 70

total_original_bytes = 0
total_rewritten_bytes = 0
total_rows = 0

matched_files.each_with_index do |file_path, idx|
  file_name = File.basename(file_path)
  orig_size = File.size(file_path)
  total_original_bytes += orig_size

  print "[#{idx + 1}/#{matched_files.size}] #{file_name} (#{orig_size / 1024} KB)... "

  if options[:dry_run]
    puts "[DRY RUN]"
    next
  end

  tmp_path = "#{file_path}.tmp"
  escaped_path = file_path.to_s.gsub("'", "''")
  escaped_tmp = tmp_path.to_s.gsub("'", "''")

  begin
    DuckDB::Database.open do |db|
      db.connect do |con|
        orig_count = con.query("SELECT COUNT(*) FROM read_parquet('#{escaped_path}')").first.first.to_i
        total_rows += orig_count

        con.query(<<~SQL)
          COPY (
            SELECT *
            FROM read_parquet('#{escaped_path}')
            ORDER BY expiration_date, sampled_at, strike, contract_type, symbol
          ) TO '#{escaped_tmp}' (
            FORMAT PARQUET,
            CODEC '#{options[:codec]}',
            ROW_GROUP_SIZE #{options[:row_group_size]}
          )
        SQL

        new_count = con.query("SELECT COUNT(*) FROM read_parquet('#{escaped_tmp}')").first.first.to_i
        raise "Row count mismatch: original #{orig_count} vs rewritten #{new_count}" unless orig_count == new_count

        new_size = File.size(tmp_path)
        total_rewritten_bytes += new_size

        File.rename(tmp_path, file_path)

        pct_change = orig_size.positive? ? (((new_size - orig_size).to_f / orig_size) * 100).round(1) : 0
        diff_label = pct_change <= 0 ? "#{pct_change}%" : "+#{pct_change}%"
        puts "Done. #{orig_count} rows, #{orig_size / 1024} KB -> #{new_size / 1024} KB (#{diff_label})"
      end
    end
  rescue StandardError => e
    File.delete(tmp_path) if File.exist?(tmp_path)
    puts "FAILED: #{e.message}"
    raise e
  end
end

puts "=" * 70
if options[:dry_run]
  puts "Dry run complete for #{matched_files.size} file(s)."
else
  pct_total = total_original_bytes.positive? ? (((total_rewritten_bytes - total_original_bytes).to_f / total_original_bytes) * 100).round(1) : 0
  puts "Rewrite complete: #{matched_files.size} file(s), #{total_rows} rows."
  puts "Total size: #{(total_original_bytes / (1024.0 * 1024)).round(2)} MB -> #{(total_rewritten_bytes / (1024.0 * 1024)).round(2)} MB (#{pct_total}%)"
  puts "To re-publish rewritten files to MinIO, run:"
  puts "  bundle exec exe/tickrake publish-historical --datasets options"
end
