# frozen_string_literal: true

require "monitor"

module Tickrake
  class LevelOneJob
    HEARTBEAT_INTERVAL_SECONDS = 30
    SESSION_STALE_THRESHOLD_SECONDS = HEARTBEAT_INTERVAL_SECONDS * 2

    # Common field indices present across all Level 1 services.
    COMMON_FIELDS = { bid: "1", ask: "2", last: "3", bid_size: "4", ask_size: "5", volume: "8" }.freeze

    # Service-specific indices for quote_time_ms, trade_time_ms, and mark.
    SERVICE_FIELD_INDICES = {
      "LEVELONE_EQUITIES"        => { quote_time: "34", trade_time: "35", mark: "33" },
      "LEVELONE_OPTIONS"         => { quote_time: "38", trade_time: "39", mark: "37" },
      "LEVELONE_FUTURES"         => { quote_time: "10", trade_time: "11", mark: "24" },
      "LEVELONE_FUTURES_OPTIONS" => { quote_time: "10", trade_time: "11", mark: "19" },
      "LEVELONE_FOREX"           => { quote_time: "8",  trade_time: "9",  mark: "29" }
    }.freeze

    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
      @level_one_config = scheduled_job.settings
      @job_name = scheduled_job.name
      @provider = scheduled_job.provider

      @db_lock = Monitor.new
      @stop_requested = false

      db_path = Tickrake::PathSupport.expand_path(runtime.config.sqlite_path)
      FileUtils.mkdir_p(File.dirname(db_path))
      @db = Tickrake::DB.connection(db_path)

      @storage_paths = Tickrake::Storage::Paths.new(runtime.config)
      @parquet_writer = Tickrake::Storage::LevelOneParquetWriter.new
      @s3_archive = runtime.config.s3_archive
    end

    def run_session(window_start:)
      @stop_requested = false

      prune_old_flushed_rows
      recover_stranded_rows

      register_session

      client = Tickrake::ClientFactory.new(@runtime.config).build(@provider)
      stream = client.stream
      symbols = @scheduled_job.universe

      @level_one_config.services.each do |service|
        stream.on(service.downcase.to_sym, symbols: symbols, fields: :all) do |event|
          handle_event(event, service: service)
        end
      end

      stream.start_async

      heartbeat_thread = start_heartbeat_thread

      flush_loop
    ensure
      heartbeat_thread&.kill
      stream&.stop rescue nil
      flush_pending_events(flush_start: window_start, flush_end: Time.now)
      delete_session
    end

    def stop
      @stop_requested = true
    end

    def close
      # DB connection is managed by Tickrake::DB singleton; nothing to close here.
    end

    private

    def flush_loop
      flush_interval = @level_one_config.flush_interval_seconds

      until @stop_requested
        flush_start = Time.now
        flush_end   = flush_start + flush_interval

        interruptible_sleep(flush_interval)

        flush_pending_events(flush_start: flush_start, flush_end: flush_end)
      end
    end

    def handle_event(event, service:)
      received_at = (Time.now.to_f * 1000).to_i
      entries = Array(event["content"] || event[:content] || [event])

      @db_lock.synchronize do
        entries.each do |entry|
          symbol = entry["key"] || entry[:key]
          next unless symbol

          fields = extract_fields(entry, service)

          @db.execute(
            <<~SQL,
              INSERT INTO level_one_events
                (job_name, received_at, symbol, service,
                 quote_time_ms, trade_time_ms, bid, ask, last,
                 bid_size, ask_size, volume, mark, extra_json, flushed)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            SQL
            [
              @job_name, received_at, symbol.to_s, service,
              fields[:quote_time_ms], fields[:trade_time_ms],
              fields[:bid], fields[:ask], fields[:last],
              fields[:bid_size], fields[:ask_size], fields[:volume],
              fields[:mark], fields[:extra_json]
            ]
          )
        end
      end
    end

    def extract_fields(entry, service)
      indices = SERVICE_FIELD_INDICES[service] || {}

      bid       = entry[COMMON_FIELDS[:bid]]&.to_f
      ask       = entry[COMMON_FIELDS[:ask]]&.to_f
      last      = entry[COMMON_FIELDS[:last]]&.to_f
      bid_size  = entry[COMMON_FIELDS[:bid_size]]&.to_i
      ask_size  = entry[COMMON_FIELDS[:ask_size]]&.to_i
      volume    = entry[COMMON_FIELDS[:volume]]&.to_i
      quote_time_ms = indices[:quote_time] ? entry[indices[:quote_time]]&.to_i : nil
      trade_time_ms = indices[:trade_time] ? entry[indices[:trade_time]]&.to_i : nil
      mark          = indices[:mark]        ? entry[indices[:mark]]&.to_f        : nil

      extracted_keys = (COMMON_FIELDS.values + indices.values + ["key"]).uniq
      extra = entry.reject { |k, _| extracted_keys.include?(k.to_s) }
      extra_json = extra.empty? ? nil : JSON.dump(extra)

      {
        bid: bid, ask: ask, last: last,
        bid_size: bid_size, ask_size: ask_size, volume: volume,
        quote_time_ms: quote_time_ms, trade_time_ms: trade_time_ms,
        mark: mark, extra_json: extra_json
      }
    end

    def flush_pending_events(flush_start:, flush_end:)
      flush_end_ms = (flush_end.to_f * 1000).to_i

      rows = @db_lock.synchronize do
        @db.execute(
          <<~SQL,
            SELECT id, symbol, service, received_at,
                   quote_time_ms, trade_time_ms, bid, ask, last,
                   bid_size, ask_size, volume, mark, extra_json
            FROM level_one_events
            WHERE job_name = ? AND flushed = 0 AND received_at <= ?
            ORDER BY received_at
          SQL
          [@job_name, flush_end_ms]
        )
      end

      return if rows.empty?

      by_symbol = rows.group_by { |r| r.fetch("symbol") }

      flushed_ids = []
      by_symbol.each do |symbol, symbol_rows|
        path = @storage_paths.level_one_path(provider: @provider, symbol: symbol, flush_start: flush_start)
        begin
          @parquet_writer.write(path, rows: symbol_rows)
          upload_to_s3(path) if @s3_archive
          flushed_ids.concat(symbol_rows.map { |r| r.fetch("id") })
        rescue StandardError => e
          @runtime.logger.error("#{log_prefix} Failed to flush #{symbol_rows.size} rows for #{symbol}: #{e.class}: #{e.message}")
        end
      end

      return if flushed_ids.empty?

      flushed_ids.each_slice(500) do |batch|
        placeholders = (["?"] * batch.size).join(", ")
        @db_lock.synchronize do
          @db.execute("UPDATE level_one_events SET flushed = 1 WHERE id IN (#{placeholders})", batch)
        end
      end

      @runtime.logger.info("#{log_prefix} Flushed #{flushed_ids.size} events for #{by_symbol.size} symbols.")
    end

    def recover_stranded_rows
      min_received_at = @db_lock.synchronize do
        @db.get_first_value(
          "SELECT MIN(received_at) FROM level_one_events WHERE job_name = ? AND flushed = 0",
          [@job_name]
        )
      end
      return unless min_received_at

      count = @db_lock.synchronize do
        @db.get_first_value(
          "SELECT COUNT(*) FROM level_one_events WHERE job_name = ? AND flushed = 0",
          [@job_name]
        )
      end

      @runtime.logger.warn("#{log_prefix} Recovering #{count} unflushed rows from previous session.")
      recovery_start = Time.at(min_received_at.to_i / 1000.0)
      flush_pending_events(flush_start: recovery_start, flush_end: Time.now)
    end

    def prune_old_flushed_rows
      cutoff_ms = ((Time.now - (@level_one_config.retention_days * 86_400)).to_f * 1000).to_i
      @db_lock.synchronize do
        @db.execute(
          "DELETE FROM level_one_events WHERE job_name = ? AND flushed = 1 AND received_at < ?",
          [@job_name, cutoff_ms]
        )
      end
    end

    def register_session
      now_ms = (Time.now.to_f * 1000).to_i
      parameters = { services: @level_one_config.services, symbols: @scheduled_job.universe }
      @db_lock.synchronize do
        @db.execute("DELETE FROM job_sessions WHERE job_name = ?", [@job_name])
        @db.execute(
          <<~SQL,
            INSERT INTO job_sessions (job_name, job_type, provider, parameters_json, started_at, heartbeat_at)
            VALUES (?, ?, ?, ?, ?, ?)
          SQL
          [@job_name, "level_one", @provider, JSON.dump(parameters), now_ms, now_ms]
        )
      end
    end

    def delete_session
      @db_lock.synchronize do
        @db.execute("DELETE FROM job_sessions WHERE job_name = ?", [@job_name])
      end
    rescue StandardError => e
      @runtime.logger.error("#{log_prefix} Failed to delete session record: #{e.message}")
    end

    def start_heartbeat_thread
      Thread.new do
        loop do
          sleep(HEARTBEAT_INTERVAL_SECONDS)
          break if @stop_requested

          now_ms = (Time.now.to_f * 1000).to_i
          @db_lock.synchronize do
            @db.execute(
              "UPDATE job_sessions SET heartbeat_at = ? WHERE job_name = ?",
              [now_ms, @job_name]
            )
          end
        rescue StandardError => e
          @runtime.logger.error("#{log_prefix} Heartbeat error: #{e.message}")
        end
      end
    end

    def interruptible_sleep(seconds)
      deadline = Time.now + seconds
      while Time.now < deadline
        return if @stop_requested

        sleep([deadline - Time.now, 0.25].min)
      end
    end

    def upload_to_s3(local_path)
      Tickrake::Storage::S3Archive.new(@runtime.config, archive_config: @s3_archive).upload(local_path)
    rescue StandardError => e
      @runtime.logger.error("#{log_prefix} S3 upload failed for #{local_path}: #{e.message}")
    end

    def log_prefix
      "[level_one:#{@job_name}]"
    end
  end
end
