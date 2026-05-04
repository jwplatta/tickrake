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

  def initialize(config:, ticker: nil, plan_csv_path:, apply: false, stdout: $stdout, stderr: $stderr)
    @config = config
    @ticker = ticker&.to_s&.upcase
    @plan_csv_path = File.expand_path(plan_csv_path)
    @apply = apply
    @stdout = stdout
    @stderr = stderr
  end

  def run
    plan_rows = build_plan
    write_plan_csv(plan_rows)

    @stdout.puts("Wrote migration plan to #{@plan_csv_path}")
    unless @apply
      @stdout.puts("Dry run only. No files or SQLite metadata were changed#{selection_summary}.")
      return
    end

    moved_count = 0
    skipped_count = 0

    plan_rows.each do |row|
      if row.fetch(:action) == "migrate"
        move_with_metadata_update(row)
        moved_count += 1
      else
        skipped_count += 1
        @stdout.puts("Skipped #{row.fetch(:source_path)} because target already exists: #{row.fetch(:target_path)}")
      end
    end

    @stdout.puts("Moved #{moved_count} Schwab option snapshot files to UTC filenames#{selection_summary}.")
    @stdout.puts("Skipped #{skipped_count} files already migrated or blocked by existing targets.") if skipped_count.positive?
  end

  private

  def build_plan
    rows = []

    each_option_snapshot_from_metadata do |source_path, metadata_row|
      target_path, observed_at, metadata = target_path_for(source_path)

      rows << {
        action: source_path == target_path || File.exist?(target_path) ? "skip" : "migrate",
        reason: source_path == target_path ? "already_utc_filename" : (File.exist?(target_path) ? "target_exists" : "rename_to_utc"),
        ticker: metadata.fetch(:root),
        expiration_date: metadata.fetch(:expiration_date),
        source_path: source_path,
        target_path: target_path,
        source_first_observed_at: metadata_row && metadata_row["first_observed_at"],
        source_last_observed_at: metadata_row && metadata_row["last_observed_at"],
        target_first_observed_at: observed_at.iso8601,
        target_last_observed_at: observed_at.iso8601,
        metadata_present: !metadata_row.nil?
      }
    end

    rows
  end

  def each_option_snapshot_from_metadata
    tracker.file_metadata_rows(
      where: metadata_where_clause,
      binds: metadata_binds,
      order_by: "expiration_date ASC, ticker ASC, path ASC"
    ).each do |metadata_row|
      source_path = metadata_row.fetch("path")
      next unless File.file?(source_path)
      next unless option_snapshot?(source_path)

      yield(source_path, metadata_row)
    end
  end

  def metadata_where_clause
    clauses = ["dataset_type = ?", "provider_name = ?"]
    clauses << "ticker = ?" if @ticker
    clauses.join(" AND ")
  end

  def metadata_binds
    binds = ["options", "schwab"]
    binds << @ticker if @ticker
    binds
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
    [target_path, observed_at, metadata]
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

  def move_with_metadata_update(row)
    source_path = row.fetch(:source_path)
    target_path = row.fetch(:target_path)
    observed_at = Time.iso8601(row.fetch(:target_first_observed_at)).utc
    metadata_row = tracker.file_metadata(source_path)

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
  rescue StandardError => e
    tracker_db.rollback if tracker_db.transaction_active?
    rollback_move(target_path, source_path)
    raise e
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

  def write_plan_csv(rows)
    FileUtils.mkdir_p(File.dirname(@plan_csv_path))
    CSV.open(@plan_csv_path, "wb") do |csv|
      csv << %w[
        action
        reason
        ticker
        expiration_date
        source_path
        target_path
        source_first_observed_at
        source_last_observed_at
        target_first_observed_at
        target_last_observed_at
        metadata_present
      ]
      rows.each do |row|
        csv << [
          row.fetch(:action),
          row.fetch(:reason),
          row.fetch(:ticker),
          row.fetch(:expiration_date),
          row.fetch(:source_path),
          row.fetch(:target_path),
          row[:source_first_observed_at],
          row[:source_last_observed_at],
          row.fetch(:target_first_observed_at),
          row.fetch(:target_last_observed_at),
          row.fetch(:metadata_present)
        ]
      end
    end
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

  def selection_summary
    @ticker ? " for ticker #{@ticker}" : " for all Schwab option snapshots"
  end
end

if $PROGRAM_NAME == __FILE__
  options = {
    config_path: Tickrake::PathSupport.config_path,
    ticker: nil,
    plan_csv_path: nil,
    apply: false
  }

  OptionParser.new do |parser|
    parser.banner = "Usage: ruby scripts/migrate_schwab_option_filenames_to_utc.rb [--config path/to/tickrake.yml] --plan-csv path/to/plan.csv [--ticker SYMBOL] [--apply]"
    parser.on("--config PATH", "Tickrake config path") { |value| options[:config_path] = value }
    parser.on("--plan-csv PATH", "Write planned file and metadata changes to CSV before applying them") { |value| options[:plan_csv_path] = value }
    parser.on("--ticker SYMBOL", "Only plan and migrate one option root ticker, such as SPXW") { |value| options[:ticker] = value }
    parser.on("--apply", "Apply the migration after writing the plan CSV. Without this flag the script is dry-run only.") do
      options[:apply] = true
    end
  end.parse!

  raise OptionParser::MissingArgument, "--plan-csv" if options[:plan_csv_path].to_s.empty?

  config = Tickrake::ConfigLoader.load(options[:config_path])
  SchwabOptionFilenameUtcMigrator.new(
    config: config,
    ticker: options[:ticker],
    plan_csv_path: options[:plan_csv_path],
    apply: options[:apply]
  ).run
end
