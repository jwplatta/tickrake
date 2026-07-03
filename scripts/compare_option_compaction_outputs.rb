#!/usr/bin/env ruby
# frozen_string_literal: true

require "date"
require "optparse"
require_relative "../lib/tickrake"

options = {
  duckdb_path: nil,
  ruby_path: nil
}

parser = OptionParser.new do |opts|
  opts.banner = "Usage: bundle exec ruby scripts/compare_option_compaction_outputs.rb --duckdb PATH --ruby PATH"
  opts.on("--duckdb PATH") { |value| options[:duckdb_path] = value }
  opts.on("--ruby PATH") { |value| options[:ruby_path] = value }
end

parser.parse!(ARGV)

raise Tickrake::Error, "--duckdb is required." if options[:duckdb_path].to_s.empty?
raise Tickrake::Error, "--ruby is required." if options[:ruby_path].to_s.empty?

duckdb_path = File.expand_path(options[:duckdb_path])
ruby_path = File.expand_path(options[:ruby_path])

def sql_string(value)
  "'#{value.to_s.gsub("'", "''")}'"
end

def normalized_select(path, ruby_source:)
  expiration_expr = ruby_source ? "TRY_CAST(expiration_date AS DATE) AS expiration_date" : "expiration_date"
  sampled_at_expr = "CAST(sampled_at AS TIMESTAMP WITH TIME ZONE) AS sampled_at"

  <<~SQL
    SELECT
      contract_type,
      symbol,
      description,
      strike,
      #{expiration_expr},
      open,
      high,
      low,
      close,
      mark,
      bid,
      bid_size,
      ask,
      ask_size,
      last,
      last_size,
      open_interest,
      total_volume,
      transactions,
      delta,
      gamma,
      theta,
      vega,
      rho,
      volatility,
      theoretical_volatility,
      theoretical_option_value,
      intrinsic_value,
      extrinsic_value,
      underlying_price,
      #{sampled_at_expr}
    FROM read_parquet(#{sql_string(path)})
  SQL
end

DuckDB::Database.open do |db|
  db.connect do |con|
    con.query("CREATE TEMP VIEW duckdb_rows AS #{normalized_select(duckdb_path, ruby_source: false)}")
    con.query("CREATE TEMP VIEW ruby_rows AS #{normalized_select(ruby_path, ruby_source: true)}")

    duckdb_count = con.query("SELECT COUNT(*) FROM duckdb_rows").first.first
    ruby_count = con.query("SELECT COUNT(*) FROM ruby_rows").first.first
    duckdb_minus_ruby = con.query("SELECT COUNT(*) FROM (SELECT * FROM duckdb_rows EXCEPT SELECT * FROM ruby_rows)").first.first
    ruby_minus_duckdb = con.query("SELECT COUNT(*) FROM (SELECT * FROM ruby_rows EXCEPT SELECT * FROM duckdb_rows)").first.first

    puts "duckdb_count=#{duckdb_count}"
    puts "ruby_count=#{ruby_count}"
    puts "duckdb_minus_ruby=#{duckdb_minus_ruby}"
    puts "ruby_minus_duckdb=#{ruby_minus_duckdb}"

    if duckdb_minus_ruby.to_i.zero? && ruby_minus_duckdb.to_i.zero?
      puts "logical_rows_match=true"
    else
      puts "logical_rows_match=false"
      puts
      puts "sample_duckdb_minus_ruby:"
      con.query("SELECT * FROM (SELECT * FROM duckdb_rows EXCEPT SELECT * FROM ruby_rows) LIMIT 5").each { |row| p row }
      puts
      puts "sample_ruby_minus_duckdb:"
      con.query("SELECT * FROM (SELECT * FROM ruby_rows EXCEPT SELECT * FROM duckdb_rows) LIMIT 5").each { |row| p row }
    end
  end
end
