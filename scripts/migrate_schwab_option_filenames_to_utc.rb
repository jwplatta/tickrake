#!/usr/bin/env ruby
# frozen_string_literal: true

require "csv"
require "fileutils"
require "optparse"
require "sqlite3"
require "time"
require "tzinfo"

require_relative "../lib/tickrake"

class SchwabOptionFilenameUtcMigrator
  FILENAME_PATTERN = /\A(?<root>.+)_exp(?<expiration_date>\d{4}-\d{2}-\d{2})_(?<sample_date>\d{4}-\d{2}-\d{2})_(?<sample_time>\d{2}-\d{2}-\d{2})\.csv\z/.freeze

  def initialize(config:, stdout: $stdout, stderr: $stderr)
    @config = config
    @stdout = stdout
    @stderr = stderr
  end

  def run
    moved_count = 0
    skipped_count = 0

    each_option_snapshot do |source_path|
      target_path, observed_at = target_path_for(source_path)
      if source_path == target_path
        skipped_count += 1
        next
      end

      result = move_with_metadata_update(source_path, target_path, observed_at)
      result == :skipped ? skipped_count += 1 : moved_count += 1
    end

    @stdout.puts("Moved #{moved_count} Schwab option snapshot files to UTC filenames.")
    @stdout.puts("Skipped #{skipped_count} files already migrated or blocked by existing targets.") if skipped_count.positive?
  end

  private

  def each_option_snapshot
    return unless Dir.exist?(provider_dir)

    Dir.glob(File.join(provider_dir, "**", "*.csv")).sort.each do |candidate|
      next unless File.file?(candidate)
      next unless option_snapshot?(candidate)
      next if already_migrated?(candidate)

      yield(candidate)
    end
  end

  def provider_dir
    @provider_dir ||= File.join(@config.options_dir, "schwab")
  end

  def option_snapshot?(path)
    FILENAME_PATTERN.match?(File.basename(path))
  end

  def target_path_for(source_path)
    metadata = parse_snapshot_filename(source_path)
    observed_at = local_filename_time_to_utc(metadata.fetch(:sample_date), metadata.fetch(:sample_time))
    target_path = File.join(
      provider_dir,
      observed_at.strftime("%Y"),
      observed_at.strftime("%m"),
      observed_at.strftime("%d"),
      [
        metadata.fetch(:root),
        "exp#{metadata.fetch(:expiration_date)}",
        observed_at.strftime("%Y-%m-%d_%H-%M-%S")
      ].join("_") + ".csv"
    )
    [target_path, observed_at]
  end

  def parse_snapshot_filename(path)
    match = FILENAME_PATTERN.match(File.basename(path))
    raise Tickrake::Error, "Unrecognized option snapshot filename: #{path}" unless match

    {
      root: match[:root],
      expiration_date: match[:expiration_date],
      sample_date: match[:sample_date],
      sample_time: match[:sample_time]
    }
  end

  def local_filename_time_to_utc(sample_date, sample_time)
    date = Date.iso8601(sample_date)
    hour, minute, second = sample_time.split("-").map { |value| Integer(value, 10) }
    local_time = Time.new(date.year, date.month, date.day, hour, minute, second)
    offset = timezone.period_for_local(local_time, true).utc_total_offset
    Time.new(date.year, date.month, date.day, hour, minute, second, offset).utc
  end

  def move_with_metadata_update(source_path, target_path, observed_at)
    metadata_row = tracker.file_metadata(source_path)
    if File.exist?(target_path)
      @stdout.puts("Skipped #{source_path} because target already exists: #{target_path}")
      return :skipped
    end

    FileUtils.mkdir_p(File.dirname(target_path))
    tracker_db.transaction
    FileUtils.mv(source_path, target_path)
    if metadata_row
      stat = File.stat(target_path)
      tracker_db.execute(
        <<~SQL,
          UPDATE file_metadata_cache
          SET path = ?, first_observed_at = ?, last_observed_at = ?, file_mtime = ?, file_size = ?, updated_at = ?
          WHERE path = ?
        SQL
        [target_path, observed_at.iso8601, observed_at.iso8601, stat.mtime.to_i, stat.size, Time.now.utc.iso8601, source_path]
      )
    else
      tracker.upsert_file_metadata(inferred_metadata_for(target_path, observed_at))
      @stdout.puts("Inserted fresh metadata row for #{target_path}")
    end
    tracker_db.commit
    @stdout.puts("Moved #{source_path} -> #{target_path}")
    :moved
  rescue StandardError => e
    tracker_db.rollback if tracker_db.transaction_active?
    rollback_move(target_path, source_path)
    raise e
  end

  def already_migrated?(path)
    metadata = tracker.file_metadata(path)
    return false unless metadata

    observed_at = metadata["last_observed_at"] || metadata["first_observed_at"]
    return false if observed_at.to_s.empty?

    expected_path = utc_path_for(path, Time.iso8601(observed_at).utc)
    path == expected_path
  rescue ArgumentError
    false
  end

  def rollback_move(target_path, source_path)
    return unless File.exist?(target_path)
    return if File.exist?(source_path)

    FileUtils.mkdir_p(File.dirname(source_path))
    FileUtils.mv(target_path, source_path)
    @stderr.puts("Rolled back move for #{source_path}")
  end

  def inferred_metadata_for(path, observed_at)
    parsed = parse_snapshot_filename(path)
    stat = File.stat(path)

    {
      path: path,
      dataset_type: "options",
      provider_name: "schwab",
      ticker: parsed.fetch(:root),
      frequency: nil,
      expiration_date: parsed.fetch(:expiration_date),
      row_count: csv_row_count(path),
      first_observed_at: observed_at.iso8601,
      last_observed_at: observed_at.iso8601,
      file_mtime: stat.mtime.to_i,
      file_size: stat.size,
      updated_at: Time.now
    }
  end

  def utc_path_for(path, observed_at)
    parsed = parse_snapshot_filename(path)
    File.join(
      provider_dir,
      observed_at.strftime("%Y"),
      observed_at.strftime("%m"),
      observed_at.strftime("%d"),
      [
        parsed.fetch(:root),
        "exp#{parsed.fetch(:expiration_date)}",
        observed_at.strftime("%Y-%m-%d_%H-%M-%S")
      ].join("_") + ".csv"
    )
  end

  def csv_row_count(path)
    count = 0
    CSV.foreach(path, headers: true) { |_row| count += 1 }
    count
  end

  def tracker
    @tracker ||= Tickrake::Tracker.new(@config.sqlite_path)
  end

  def tracker_db
    @tracker_db ||= SQLite3::Database.new(@config.sqlite_path).tap do |database|
      tracker
      database.results_as_hash = true
      database.busy_timeout(Tickrake::Tracker::SQLITE_BUSY_TIMEOUT_MS)
    end
  end

  def timezone
    @timezone ||= TZInfo::Timezone.get(@config.timezone)
  end
end

if $PROGRAM_NAME == __FILE__
  options = { config_path: Tickrake::PathSupport.config_path }

  OptionParser.new do |parser|
    parser.banner = "Usage: ruby scripts/migrate_schwab_option_filenames_to_utc.rb [--config path/to/tickrake.yml]"
    parser.on("--config PATH", "Tickrake config path") { |value| options[:config_path] = value }
  end.parse!

  config = Tickrake::ConfigLoader.load(options[:config_path])
  SchwabOptionFilenameUtcMigrator.new(config: config).run
end
