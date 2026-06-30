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
result = Tickrake::DeleteCompactedOptionSampleCsvRange.new(
  config: config,
  tracker: tracker,
  option_root: options[:option_root],
  provider_name: options[:provider_name],
  start_date: options[:start_date],
  end_date: options[:end_date],
  dry_run: options[:dry_run],
  stdout: $stdout,
  stderr: $stderr
).run

exit(result.errors.empty? ? 0 : 1)
