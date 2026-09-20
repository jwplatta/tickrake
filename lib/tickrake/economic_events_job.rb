# frozen_string_literal: true

module Tickrake
  class EconomicEventsJob
    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
    end

    def run(now: Time.now)
      collection_id = "economic_events-#{now.utc.strftime("%Y%m%dT%H%M%SZ")}"
      @runtime.logger.info({ msg: "scrape_start", event: "scrape_start", data_type: "economic_events", collection_id: collection_id })

      from_date, to_date = fetch_window(now)
      started_at = Time.now
      total_rows = 0
      failure_count = 0

      categories = settings.fetch("categories", %w[economic earnings fomc])

      if categories.include?("economic")
        total_rows += run_bls(from_date, to_date, now)
        total_rows += run_fred(from_date, to_date, now)
      end

      if categories.include?("fomc")
        total_rows += run_fomc(from_date, to_date, now)
      end

      if categories.include?("earnings")
        total_rows += run_earnings(from_date, to_date, now)
      end

      elapsed_ms = ((Time.now - started_at) * 1000).round
      @runtime.logger.info({
        msg: "scrape_end", event: "scrape_end", data_type: "economic_events",
        row_count: total_rows, duration_ms: elapsed_ms, collection_id: collection_id
      })

      ScheduledRunResult.new(success_count: total_rows, failure_count: failure_count)
    rescue StandardError => e
      elapsed_ms = ((Time.now - started_at) * 1000).round rescue 0
      @runtime.logger.error({
        msg: "scrape_failed", event: "scrape_failed", data_type: "economic_events",
        error_class: e.class.name, error_message: e.message,
        duration_ms: elapsed_ms, collection_id: collection_id
      })
      ScheduledRunResult.new(success_count: 0, failure_count: 1)
    end

    private

    def fetch_window(now)
      lookback  = settings.fetch("lookback_days", 30)
      lookahead = settings.fetch("lookahead_days", 90)
      from_date = (now - lookback  * 86_400).to_date
      to_date   = (now + lookahead * 86_400).to_date
      [from_date, to_date]
    end

    def run_bls(from_date, to_date, now)
      rows = Fetchers::BlsCalendar.new.fetch(from_date: from_date, to_date: to_date)
      write_events(rows, source: "bls", category: "economic", now: now)
      rows.length
    rescue StandardError => e
      @runtime.logger.error({ msg: "economic_events_source_failed", source: "bls", error: e.message })
      0
    end

    def run_fred(from_date, to_date, now)
      rows = Fetchers::FredEconomicCalendar.new(logger: @runtime.logger).fetch(from_date: from_date, to_date: to_date)
      write_events(rows, source: "fred", category: "economic", now: now)
      rows.length
    rescue StandardError => e
      @runtime.logger.error({ msg: "economic_events_source_failed", source: "fred", error: e.message })
      0
    end

    def run_fomc(from_date, to_date, now)
      rows = Fetchers::FomcCalendar.new(logger: @runtime.logger).fetch(from_date: from_date, to_date: to_date)
      write_events(rows, source: "fred", category: "fomc", now: now)
      rows.length
    rescue StandardError => e
      @runtime.logger.error({ msg: "economic_events_source_failed", source: "fomc", error: e.message })
      0
    end

    def run_earnings(from_date, to_date, now)
      rows = Fetchers::AlphaVantageEarnings.new.fetch(from_date: from_date, to_date: to_date)
      write_events(rows, source: "alpha_vantage", category: "earnings", now: now)
      rows.length
    rescue StandardError => e
      @runtime.logger.error({ msg: "economic_events_source_failed", source: "alpha_vantage", error: e.message })
      0
    end

    def write_events(rows, source:, category:, now:)
      return if rows.empty?

      grouped = rows.group_by do |row|
        Date.parse(row[:event_datetime][0, 10])
      end

      grouped.each do |event_date, date_rows|
        path = storage_paths.economic_events_path(source: source, category: category, event_date: event_date)
        writer.write(path, rows: date_rows)
        @runtime.logger.info({
          msg: "economic_events_written", source: source, category: category,
          event_date: event_date.iso8601, row_count: date_rows.length, path: path
        })
      end
    end

    def settings
      @scheduled_job&.settings || {}
    end

    def storage_paths
      @storage_paths ||= Storage::Paths.new(@runtime.config)
    end

    def writer
      @writer ||= Storage::EconomicEventsParquetWriter.new
    end
  end
end
