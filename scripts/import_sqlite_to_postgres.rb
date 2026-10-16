#!/usr/bin/env ruby
# frozen_string_literal: true

# One-time script to copy existing Tickrake SQLite state into a target PostgreSQL database.
#
# Usage:
#   bundle exec ruby scripts/import_sqlite_to_postgres.rb [options]
#
# Options:
#   --sqlite-path PATH      Path to source SQLite database (default: from config)
#   --postgres-url URL      PostgreSQL connection URL (or from config database.url)
#   --config PATH           Path to tickrake.yml (default: ~/.tickrake/tickrake.yml)
#   --dry-run               Preview source row counts without modifying PostgreSQL
#   --force                 Skip confirmation prompt

require "optparse"
require "sqlite3"
require "pg"

require_relative "../lib/tickrake"

class SqliteToPostgresMigrator
  TABLES_IN_ORDER = %w[
    tickers
    market_indexes
    ticker_aliases
    market_index_memberships
    fetch_runs
    file_metadata_cache
    api_rate_limits
  ].freeze

  TABLE_SEQUENCES = {
    "tickers" => "id",
    "market_indexes" => "id",
    "ticker_aliases" => "id",
    "market_index_memberships" => "id",
    "fetch_runs" => "id"
  }.freeze

  attr_reader :sqlite_db, :pg_config, :dry_run, :stdout

  def initialize(sqlite_path:, pg_config: nil, pg_adapter: nil, dry_run: false, stdout: $stdout)
    @sqlite_path = Tickrake::PathSupport.expand_path(sqlite_path)
    @pg_config = pg_config || (pg_adapter.respond_to?(:config) ? pg_adapter.config : pg_adapter)
    @dry_run = dry_run
    @stdout = stdout
  end

  def run
    unless File.exist?(@sqlite_path)
      raise Tickrake::Error, "Source SQLite database does not exist: #{@sqlite_path}"
    end

    @sqlite_db = SQLite3::Database.new(@sqlite_path)
    @sqlite_db.results_as_hash = true

    stdout.puts "Connected to source SQLite: #{@sqlite_path}"
    stdout.puts "Checking PostgreSQL schema and row counts..."

    # Ensure schema migrations have run on postgres target
    Tickrake::Tracker.migrate!(@pg_config)
    connection = Tickrake::DB::ConnectionManager.establish_connection(@pg_config)

    # Empty target check
    verify_empty_target!(connection)

    source_counts = fetch_source_counts
    stdout.puts "\nSource table row counts:"
    TABLES_IN_ORDER.each do |table|
      stdout.puts "  - #{table}: #{source_counts[table]} rows"
    end

    if dry_run
      stdout.puts "\n[DRY RUN] No changes were written to PostgreSQL."
      return
    end

    stdout.puts "\nCopying data to PostgreSQL..."
    connection.transaction do
      TABLES_IN_ORDER.each do |table|
        count = copy_table(connection, table)
        stdout.puts "  Copied #{count} rows into #{table}"
      end

      # Reset sequences
      stdout.puts "\nResetting PostgreSQL ID sequences..."
      TABLE_SEQUENCES.each do |table, pk|
        connection.reset_pk_sequence!(table, pk) rescue nil
        stdout.puts "  Reset sequence for #{table}.#{pk}"
      end
    end

    # Verification check
    stdout.puts "\nVerifying row counts..."
    verify_copied_counts!(connection, source_counts)

    stdout.puts "\nSuccessfully migrated SQLite state to PostgreSQL!"
  ensure
    @sqlite_db&.close rescue nil
  end

  private

  def verify_empty_target!(connection)
    non_empty = []
    TABLES_IN_ORDER.each do |table|
      next unless connection.table_exists?(table)

      count = connection.select_value("SELECT COUNT(*) FROM #{table}").to_i
      non_empty << "#{table} (#{count} rows)" if count.positive?
    end

    return if non_empty.empty?

    raise Tickrake::Error,
          "Target PostgreSQL database is not empty. The following tables contain data:\n  - #{non_empty.join("\n  - ")}\n" \
          "Migration requires an empty target to prevent data duplication or ID collisions."
  end

  def fetch_source_counts
    TABLES_IN_ORDER.each_with_object({}) do |table, counts|
      counts[table] = if sqlite_table_exists?(table)
                        @sqlite_db.get_first_value("SELECT COUNT(*) FROM #{table}").to_i
                      else
                        0
                      end
    end
  end

  def sqlite_table_exists?(table)
    val = @sqlite_db.get_first_value(
      "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
      [table]
    )
    !val.nil?
  end

  def copy_table(connection, table)
    return 0 unless sqlite_table_exists?(table)

    rows = @sqlite_db.execute("SELECT * FROM #{table}")
    return 0 if rows.empty?

    columns = rows.first.keys
    quoted_table = connection.quote_table_name(table)
    quoted_columns = columns.map { |c| connection.quote_column_name(c) }.join(", ")

    rows.each_slice(500) do |batch|
      values_list = batch.map do |row|
        "(" + columns.map { |c| connection.quote(row[c]) }.join(", ") + ")"
      end
      connection.execute("INSERT INTO #{quoted_table} (#{quoted_columns}) VALUES #{values_list.join(", ")}")
    end

    rows.length
  end

  def verify_copied_counts!(connection, source_counts)
    mismatches = []
    TABLES_IN_ORDER.each do |table|
      pg_count = connection.select_value("SELECT COUNT(*) FROM #{table}").to_i
      expected = source_counts[table]
      if pg_count != expected
        mismatches << "#{table}: expected #{expected}, got #{pg_count}"
      end
    end

    return if mismatches.empty?

    raise Tickrake::Error, "Migration verification failed! Mismatched table counts:\n  - #{mismatches.join("\n  - ")}"
  end
end

if __FILE__ == $PROGRAM_NAME
  options = {
    sqlite_path: nil,
    postgres_url: nil,
    config_path: Tickrake::PathSupport.config_path,
    dry_run: false,
    force: false
  }

  OptionParser.new do |opts|
    opts.banner = "Usage: ruby scripts/import_sqlite_to_postgres.rb [options]"
    opts.on("--sqlite-path PATH", "Path to source SQLite database") { |v| options[:sqlite_path] = v }
    opts.on("--postgres-url URL", "Target PostgreSQL connection URL") { |v| options[:postgres_url] = v }
    opts.on("--config PATH", "Path to tickrake.yml") { |v| options[:config_path] = v }
    opts.on("--dry-run", "Preview migration counts without writing") { options[:dry_run] = true }
    opts.on("--force", "Proceed without interactive confirmation") { options[:force] = true }
  end.parse!

  config = if File.exist?(options[:config_path])
             Tickrake::ConfigLoader.load(options[:config_path])
           end

  sqlite_path = options[:sqlite_path] || config&.sqlite_path || Tickrake::PathSupport.sqlite_path

  pg_config = if options[:postgres_url]
                Tickrake::DatabaseConfig.new(backend: "postgres", url: options[:postgres_url])
              elsif config&.database&.postgres?
                config.database
              else
                raise Tickrake::Error, "No PostgreSQL target configured. Specify --postgres-url or configure postgres in #{options[:config_path]}."
              end

  unless options[:dry_run] || options[:force]
    puts "About to migrate SQLite state from #{sqlite_path} to PostgreSQL (#{pg_config.database || pg_config.url})."
    print "Are you sure you want to proceed? (y/N): "
    answer = $stdin.gets&.strip
    unless answer =~ /\A[yY]/
      puts "Aborted."
      exit 1
    end
  end

  migrator = SqliteToPostgresMigrator.new(
    sqlite_path: sqlite_path,
    pg_config: pg_config,
    dry_run: options[:dry_run]
  )

  migrator.run
end
