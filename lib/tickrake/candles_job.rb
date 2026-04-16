# frozen_string_literal: true

module Tickrake
  class CandlesJob
    def initialize(runtime, from_config_start: false, universe: nil, start_date_override: nil, end_date_override: nil, progress_output: nil)
      @runtime = runtime
      @from_config_start = from_config_start
      @universe = universe
      @start_date_override = start_date_override
      @end_date_override = end_date_override
      @progress_output = progress_output
    end

    def run(now: Time.now)
      @runtime.logger.info("Starting candle scrape at #{now.utc.iso8601}")
      selected_universe.each do |entry|
        provider = provider_for(entry)
        provider_definition = provider_definition_for(entry)
        entry.frequencies.each do |frequency|
          fetch_one(entry, frequency, provider, provider_definition, now)
        end
      end
      @runtime.logger.info("Completed candle scrape at #{Time.now.utc.iso8601}")
    end

    private

    def fetch_one(entry, frequency, provider, provider_definition, scheduled_for)
      canonical_symbol = canonical_symbol_for(entry.symbol, provider_definition)
      @runtime.logger.info("Fetching #{frequency} candles for #{entry.symbol}")
      id = @runtime.tracker.record_start(
        job_type: "eod_candles",
        dataset_type: "candles",
        symbol: canonical_symbol,
        frequency: frequency,
        scheduled_for: scheduled_for,
        started_at: Time.now
      )

      retries = 0
      begin
        start_date = request_start_date(entry, frequency, provider_definition, scheduled_for)
        end_date = request_end_date(scheduled_for)
        path = storage_paths.candle_path(provider: provider.provider_name, symbol: canonical_symbol, frequency: frequency)
        ranges = request_ranges(provider: provider, frequency: frequency, start_date: start_date, end_date: end_date)
        progress_reporter = build_progress_reporter(entry: entry, frequency: frequency, total: ranges.length)
        total_candles = 0

        ranges.each_with_index do |(chunk_start, chunk_end), index|
          @runtime.logger.info(
            "Fetching #{frequency} candle chunk #{index + 1}/#{ranges.length} for #{entry.symbol} (#{chunk_start.iso8601} to #{chunk_end.iso8601})"
          ) if ranges.length > 1

          fetched_bars = Timeout.timeout(@runtime.config.candle_fetch_timeout_seconds) do
            provider.fetch_bars(
              symbol: entry.symbol,
              frequency: frequency,
              start_date: chunk_start,
              end_date: chunk_end,
              extended_hours: entry.need_extended_hours_data,
              previous_close: entry.need_previous_close
            )
          end
          candle_reconciler.write(path: path, bars: fetched_bars)
          total_candles += Array(fetched_bars).size
          progress_reporter&.advance(title: progress_title(entry: entry, frequency: frequency, index: index, total: ranges.length))
        end

        @runtime.logger.info(
          "Wrote #{frequency} candles for #{entry.symbol} to #{path} (requested #{start_date.iso8601} to #{end_date.iso8601}, #{total_candles} rows)"
        )
        @runtime.tracker.record_finish(id: id, status: "success", finished_at: Time.now, output_path: path)
        progress_reporter&.finish
      rescue StandardError => e
        retries += 1
        if retries <= @runtime.config.retry_count
          @runtime.logger.warn("Retry #{retries} for #{entry.symbol} #{frequency}: #{e.message}")
          sleep @runtime.config.retry_delay_seconds
          retry
        end
        @runtime.logger.error("Failed candle fetch for #{entry.symbol} #{frequency}: #{e.message}")
        @runtime.tracker.record_finish(id: id, status: "failed", finished_at: Time.now, error_message: e.message)
        progress_reporter&.finish
      end
    end

    def build_progress_reporter(entry:, frequency:, total:)
      return unless @progress_output

      Tickrake::ProgressReporter.build(
        total: total,
        title: progress_title(entry: entry, frequency: frequency, index: 0, total: total),
        output: @progress_output
      )
    end

    def progress_title(entry:, frequency:, index:, total:)
      base = "#{entry.symbol} #{frequency}"
      return base if total <= 1

      "#{base} chunk #{index + 1}/#{total}"
    end

    def request_start_date(entry, frequency, provider_definition, scheduled_for)
      return @start_date_override if @start_date_override
      return entry.start_date if @from_config_start
      return lookback_start_date(entry, scheduled_for) if ibkr_intraday_frequency?(frequency, provider_definition)
      return entry.start_date unless File.exist?(history_path(entry, frequency, provider_definition))

      lookback_start_date(entry, scheduled_for)
    end

    def history_path(entry, frequency, provider_definition)
      storage_paths.candle_path(
        provider: provider_name_for(entry),
        symbol: canonical_symbol_for(entry.symbol, provider_definition),
        frequency: frequency
      )
    end

    def storage_paths
      @storage_paths ||= Storage::Paths.new(@runtime.config)
    end

    def canonical_symbol_for(symbol, provider_definition)
      symbol_normalizer.canonical(symbol, provider_definition: provider_definition)
    end

    def symbol_normalizer
      @symbol_normalizer ||= Tickrake::Query::SymbolNormalizer.new
    end

    def selected_universe
      @universe || @runtime.config.candles_universe
    end

    def candle_reconciler
      @candle_reconciler ||= Storage::CandleReconciler.new
    end

    def lookback_start_date(entry, scheduled_for)
      lookback_start = scheduled_for.to_date - @runtime.config.candle_lookback_days
      [entry.start_date, lookback_start].max
    end

    def ibkr_intraday_frequency?(frequency, provider_definition)
      provider_definition.adapter == "ibkr" && !%w[day week month].include?(frequency)
    end

    def request_ranges(provider:, frequency:, start_date:, end_date:)
      chunk_days = provider_chunk_days(provider: provider, frequency: frequency)
      return [[start_date, end_date]] unless chunk_days

      ranges = []
      cursor = start_date
      while cursor <= end_date
        chunk_end = [cursor + (chunk_days - 1), end_date].min
        ranges << [cursor, chunk_end]
        cursor = chunk_end + 1
      end
      ranges
    end

    def provider_chunk_days(provider:, frequency:)
      return unless provider.adapter_name == "ibkr"

      {
        "1min" => 6,
        "5min" => 6,
        "10min" => 6,
        "15min" => 20,
        "30min" => 34,
        "day" => 365,
        "week" => 365,
        "month" => 365
      }.fetch(frequency, nil)
    end

    def request_end_date(scheduled_for)
      return @end_date_override + 1 if @end_date_override

      scheduled_for.to_date + 1
    end

    def provider_definition_for(entry)
      @runtime.config.provider_definition(provider_name_for(entry))
    end

    def provider_for(entry)
      provider_name = provider_name_for(entry)
      @providers ||= {}
      return @providers[provider_name] if @providers.key?(provider_name)

      @providers[provider_name] =
        if provider_name == @runtime.provider_name
          @runtime.provider_factory.build
        else
          Tickrake::ProviderFactory.new(
            @runtime.config,
            provider_name: provider_name,
            client_factory: @runtime.client_factory
          ).build
        end
    end

    def provider_name_for(entry)
      @runtime.config.provider_name_with_override(@runtime.provider_name, entry)
    end
  end
end
