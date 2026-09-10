# frozen_string_literal: true

require "monitor"

module Tickrake
  class OrderBookJob
    HEARTBEAT_INTERVAL_SECONDS = 30
    SESSION_STALE_THRESHOLD_SECONDS = HEARTBEAT_INTERVAL_SECONDS * 2

    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
      @order_book_config = scheduled_job.settings
      @job_name = scheduled_job.name
      @provider = scheduled_job.provider

      @db_lock = Monitor.new
      @stop_requested = false
      @subscribed_symbols = []
      @last_resolved_at = nil

      db_path = Tickrake::PathSupport.expand_path(runtime.config.sqlite_path)
      FileUtils.mkdir_p(File.dirname(db_path))
      @db = Tickrake::DB.connection(db_path)

      @storage_paths = Tickrake::Storage::Paths.new(runtime.config)
      @provider_definition = runtime.config.provider_definition(@provider)
      @parquet_writer = Tickrake::Storage::OrderBookParquetWriter.new
      @s3_archive = runtime.config.s3_archive
    end

    def run_session(window_start:)
      @stop_requested = false
      @subscribed_symbols = []
      @last_resolved_at = nil

      prune_old_flushed_rows
      recover_stranded_rows

      register_session
      check_symbol_conflicts

      client = Tickrake::ClientFactory.new(@runtime.config).build
      stream = SchwabRb::Stream::Client.new(client)

      if @order_book_config.options_book?
        resolver = Tickrake::OrderBook::ContractResolver.new(client, @order_book_config.contracts)
        initial_symbols = resolver.resolve
        @subscribed_symbols = initial_symbols.dup
        @last_resolved_at = Time.now
        @runtime.logger.info("#{log_prefix} Resolved #{initial_symbols.size} initial contracts: #{initial_symbols.first(5).join(", ")}...")
      else
        @subscribed_symbols = @scheduled_job.universe.dup
      end

      @order_book_config.services.each do |service|
        stream.on(service.downcase.to_sym, symbols: @subscribed_symbols, fields: :all) do |event|
          handle_event(event, service: service)
        end
      end

      stream.start_async

      heartbeat_thread = start_heartbeat_thread

      flush_loop(stream: stream, resolver: @order_book_config.options_book? ? resolver : nil)
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

    def flush_loop(stream:, resolver:)
      flush_interval = @order_book_config.flush_interval_seconds

      until @stop_requested
        flush_start = Time.now
        flush_end   = flush_start + flush_interval

        interruptible_sleep(flush_interval)

        if resolver && (Time.now - @last_resolved_at) >= @order_book_config.re_resolve_interval_seconds
          new_symbols = resolver.resolve
          added = new_symbols - @subscribed_symbols
          unless added.empty?
            @runtime.logger.info("#{log_prefix} Adding #{added.size} new contracts from re-resolution.")
            @order_book_config.services.each do |service|
              stream.on(service.downcase.to_sym, symbols: added, fields: :all) do |event|
                handle_event(event, service: service)
              end
            end
            @subscribed_symbols |= added
          end
          @last_resolved_at = Time.now
        end

        flush_pending_events(flush_start: flush_start, flush_end: flush_end)
      end
    end

    def handle_event(event, service:)
      received_at = (Time.now.to_f * 1000).to_i
      entries = Array(event.try(:content) || event[:content] || event["content"] || [event])

      @db_lock.synchronize do
        entries.each do |entry|
          symbol = entry.try(:key) || entry[:key] || entry["key"] || entry.try(:symbol) || entry[:symbol] || entry["symbol"]
          next unless symbol

          @db.execute(
            <<~SQL,
              INSERT INTO order_book_events
                (job_name, received_at, symbol, service, book_time_ms, bids_json, asks_json, flushed)
              VALUES (?, ?, ?, ?, ?, ?, ?, 0)
            SQL
            [
              @job_name,
              received_at,
              symbol.to_s,
              service,
              extract_book_time(entry),
              extract_bids_json(entry),
              extract_asks_json(entry)
            ]
          )
        end
      end
    end

    def flush_pending_events(flush_start:, flush_end:)
      flush_end_ms = (flush_end.to_f * 1000).to_i

      rows = @db_lock.synchronize do
        @db.execute(
          <<~SQL,
            SELECT id, symbol, service, received_at, book_time_ms, bids_json, asks_json
            FROM order_book_events
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
        path = @storage_paths.order_book_path(provider: @provider, symbol: symbol, flush_start: flush_start, provider_definition: @provider_definition)
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
          @db.execute("UPDATE order_book_events SET flushed = 1 WHERE id IN (#{placeholders})", batch)
        end
      end

      @runtime.logger.info("#{log_prefix} Flushed #{flushed_ids.size} events for #{by_symbol.size} symbols.")
    end

    def recover_stranded_rows
      min_received_at = @db_lock.synchronize do
        @db.get_first_value(
          "SELECT MIN(received_at) FROM order_book_events WHERE job_name = ? AND flushed = 0",
          [@job_name]
        )
      end
      return unless min_received_at

      count = @db_lock.synchronize do
        @db.get_first_value(
          "SELECT COUNT(*) FROM order_book_events WHERE job_name = ? AND flushed = 0",
          [@job_name]
        )
      end

      @runtime.logger.warn("#{log_prefix} Recovering #{count} unflushed rows from previous session.")
      recovery_start = Time.at(min_received_at.to_i / 1000.0)
      flush_pending_events(flush_start: recovery_start, flush_end: Time.now)
    end

    def prune_old_flushed_rows
      cutoff_ms = ((Time.now - (@order_book_config.retention_days * 86_400)).to_f * 1000).to_i
      @db_lock.synchronize do
        @db.execute(
          "DELETE FROM order_book_events WHERE job_name = ? AND flushed = 1 AND received_at < ?",
          [@job_name, cutoff_ms]
        )
      end
    end

    def register_session
      now_ms = (Time.now.to_f * 1000).to_i
      parameters = {
        services: @order_book_config.services,
        symbols: @scheduled_job.universe
      }
      @db_lock.synchronize do
        @db.execute("DELETE FROM job_sessions WHERE job_name = ?", [@job_name])
        @db.execute(
          <<~SQL,
            INSERT INTO job_sessions (job_name, job_type, provider, parameters_json, started_at, heartbeat_at)
            VALUES (?, ?, ?, ?, ?, ?)
          SQL
          [@job_name, "order_book", @provider, JSON.dump(parameters), now_ms, now_ms]
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

    def check_symbol_conflicts
      stale_threshold_ms = (Time.now.to_f * 1000).to_i - (SESSION_STALE_THRESHOLD_SECONDS * 1000)
      live_sessions = @db_lock.synchronize do
        @db.execute(
          <<~SQL,
            SELECT job_name, parameters_json FROM job_sessions
            WHERE job_type = 'order_book'
              AND job_name != ?
              AND heartbeat_at >= ?
          SQL
          [@job_name, stale_threshold_ms]
        )
      end

      my_symbols = Set.new(@scheduled_job.universe.map(&:upcase))
      live_sessions.each do |session|
        params = JSON.parse(session.fetch("parameters_json") || "{}")
        other_symbols = Set.new(Array(params["symbols"]).map(&:upcase))
        overlap = my_symbols & other_symbols
        next if overlap.empty?

        raise Tickrake::Error,
              "order_book job `#{@job_name}` conflicts with live session `#{session.fetch("job_name")}` " \
              "on symbols: #{overlap.to_a.join(", ")}"
      end
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

    def extract_book_time(entry)
      (entry.try(:book_time) || entry[:book_time] || entry["book_time"] ||
       entry.try(:timestamp) || entry[:timestamp] || entry["timestamp"])&.to_i
    end

    def extract_bids_json(entry)
      bids = entry.try(:bids) || entry[:bids] || entry["bids"]
      JSON.dump(bids) if bids
    end

    def extract_asks_json(entry)
      asks = entry.try(:asks) || entry[:asks] || entry["asks"]
      JSON.dump(asks) if asks
    end

    def log_prefix
      "[order_book:#{@job_name}]"
    end
  end
end
