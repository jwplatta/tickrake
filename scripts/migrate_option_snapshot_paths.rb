#!/usr/bin/env ruby
# frozen_string_literal: true

require "find"
require "optparse"
require "sqlite3"

require_relative "../lib/tickrake"

class OptionSnapshotPathMigrator
  FILENAME_PATTERN = /\A(?<root>.+)_exp(?<expiration_date>\d{4}-\d{2}-\d{2})_(?<sample_date>\d{4}-\d{2}-\d{2})_(?<sample_time>\d{2}-\d{2}-\d{2})\.csv\z/.freeze

  def initialize(config:, stdout: $stdout, stderr: $stderr)
    @config = config
    @stdout = stdout
    @stderr = stderr
  end

  def run
    moved_count = 0
    skipped_count = 0

    provider_dirs.each do |provider_dir|
      each_option_snapshot(provider_dir) do |source_path, provider_name|
        target_path = target_path_for(provider_name, source_path)
        if source_path == target_path
          skipped_count += 1
          next
        end

        move_with_metadata_update(source_path, target_path)
        moved_count += 1
      end
    end

    @stdout.puts("Moved #{moved_count} option snapshot files.")
    @stdout.puts("Skipped #{skipped_count} files already in the dated layout.") if skipped_count.positive?
  end

  private

  def provider_dirs
    return [] unless Dir.exist?(@config.options_dir)

    Dir.children(@config.options_dir).sort.filter_map do |entry|
      next if entry.start_with?(".")

      path = File.join(@config.options_dir, entry)
      next unless File.directory?(path)

      path
    end
  end

  def each_option_snapshot(provider_dir)
    provider_name = File.basename(provider_dir)
    Find.find(provider_dir) do |candidate|
      next unless File.file?(candidate)
      next unless option_snapshot?(candidate)

      yield(candidate, provider_name)
    end
  end

  def option_snapshot?(path)
    FILENAME_PATTERN.match?(File.basename(path))
  end

  def target_path_for(provider_name, source_path)
    metadata = parse_snapshot_filename(source_path)
    File.join(
      @config.options_dir,
      provider_name,
      metadata.fetch(:sample_timestamp).strftime("%Y"),
      metadata.fetch(:sample_timestamp).strftime("%m"),
      metadata.fetch(:sample_timestamp).strftime("%d"),
      File.basename(source_path)
    )
  end

  def parse_snapshot_filename(path)
    match = FILENAME_PATTERN.match(File.basename(path))
    raise Tickrake::Error, "Unrecognized option snapshot filename: #{path}" unless match

    sample_timestamp = Time.iso8601("#{match[:sample_date]}T#{match[:sample_time].tr('-', ':')}Z").utc
    {
      root: match[:root],
      expiration_date: match[:expiration_date],
      sample_timestamp: sample_timestamp
    }
  end

  def move_with_metadata_update(source_path, target_path)
    metadata_row = tracker_db.get_first_row("SELECT path FROM file_metadata_cache WHERE path = ?", [source_path])
    raise Tickrake::Error, "Missing metadata row for #{source_path}" unless metadata_row
    raise Tickrake::Error, "Target already exists: #{target_path}" if File.exist?(target_path)

    FileUtils.mkdir_p(File.dirname(target_path))
    tracker_db.transaction
    FileUtils.mv(source_path, target_path)
    tracker_db.execute("UPDATE file_metadata_cache SET path = ? WHERE path = ?", [target_path, source_path])
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

  def tracker_db
    @tracker_db ||= begin
      # Reuse Tickrake's additive DB setup so the metadata cache table exists even
      # if the local runtime has not touched this SQLite file yet.
      Tickrake::Tracker.new(@config.sqlite_path)
      SQLite3::Database.new(@config.sqlite_path).tap do |database|
        database.results_as_hash = true
        database.busy_timeout(Tickrake::Tracker::SQLITE_BUSY_TIMEOUT_MS)
      end
    end
  end
end

if $PROGRAM_NAME == __FILE__
  options = { config_path: Tickrake::PathSupport.config_path }

  OptionParser.new do |parser|
    parser.banner = "Usage: ruby scripts/migrate_option_snapshot_paths.rb [--config path/to/tickrake.yml]"
    parser.on("--config PATH", "Tickrake config path") { |value| options[:config_path] = value }
  end.parse!

  config = Tickrake::ConfigLoader.load(options[:config_path])
  OptionSnapshotPathMigrator.new(config: config).run
end
