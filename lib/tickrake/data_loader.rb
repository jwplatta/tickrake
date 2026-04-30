# frozen_string_literal: true

module Tickrake
  class DataLoader
    OPTION_BUCKET_SECONDS = {
      "1min" => 60,
      "5min" => 300,
      "10min" => 600,
      "15min" => 900,
      "30min" => 1800,
      "day" => 86_400
    }.freeze
    CANDLE_FLOAT_FIELDS = %w[open high low close].freeze
    CANDLE_INTEGER_FIELDS = %w[volume].freeze
    OPTION_FLOAT_FIELDS = %w[
      strike
      open
      high
      low
      close
      mark
      bid
      ask
      last
      delta
      gamma
      theta
      vega
      rho
      volatility
      theoretical_volatility
      theoretical_option_value
      intrinsic_value
      extrinsic_value
      underlying_price
    ].freeze
    OPTION_INTEGER_FIELDS = %w[
      bid_size
      ask_size
      last_size
      open_interest
      total_volume
      transactions
    ].freeze

    def initialize(config_path: Tickrake::PathSupport.config_path, config: nil, tracker: nil)
      @config = config || Tickrake::ConfigLoader.load(config_path)
      @tracker = tracker || Tickrake::Tracker.new(@config.sqlite_path)
      @frequency_normalizer = Tickrake::Query::FrequencyNormalizer.new
    end

    def load_candles(provider:, ticker:, frequency:, start_date:, end_date:, include_metadata: false)
      results = Tickrake::Query::CandlesScanner.new(
        config: @config,
        tracker: @tracker
      ).scan(
        provider_name: provider,
        ticker: ticker,
        frequency: frequency,
        start_date: start_date,
        end_date: end_date
      )

      Enumerator.new do |yielder|
        results.each do |result|
          CSV.foreach(result.path, headers: true) do |row|
            observed_at = Time.iso8601(row.fetch("datetime")).utc
            next if before_start_date?(observed_at, start_date)
            next if after_end_date?(observed_at, end_date)

            yielder << with_metadata(
              parse_candle_row(row),
              include_metadata: include_metadata,
              metadata: {
                "dataset_type" => result.dataset_type,
                "provider_name" => result.provider_name,
                "ticker" => result.ticker,
                "source_path" => result.path,
                "frequency" => result.frequency
              }
            )
          end
        end
      end
    end

    def load_option_chains(provider:, ticker:, option_root: nil, expiration_date: nil, start_date:, end_date:, frequency: nil, bucket_selector: :last, include_metadata: false)
      normalized_selector = normalize_bucket_selector(bucket_selector)
      results = Tickrake::Query::OptionsScanner.new(
        config: @config,
        tracker: @tracker
      ).scan(
        provider_name: provider,
        ticker: option_root || ticker,
        start_date: start_date,
        end_date: end_date,
        expiration_date: expiration_date
      )
      filtered_results = filter_option_results(results, ticker: ticker, option_root: option_root)
      selected_results = select_option_results(filtered_results, frequency: frequency, bucket_selector: normalized_selector)

      Enumerator.new do |yielder|
        selected_results.each do |result|
          CSV.foreach(result.file_path, headers: true) do |row|
            yielder << with_metadata(
              parse_option_row(row),
              include_metadata: include_metadata,
              metadata: {
                "dataset_type" => result.dataset_type,
                "provider_name" => result.provider_name,
                "ticker" => result.ticker,
                "option_root" => result.root_symbol,
                "source_path" => result.file_path,
                "sampled_at" => Time.iso8601(result.sample_datetime).utc,
                "expiration_date" => Date.iso8601(result.expiration_date)
              }
            )
          end
        end
      end
    end

    private

    def before_start_date?(time, start_date)
      start_date && time < Time.utc(start_date.year, start_date.month, start_date.day)
    end

    def after_end_date?(time, end_date)
      end_date && time >= Time.utc(end_date.year, end_date.month, end_date.day) + 86_400
    end

    def filter_option_results(results, ticker:, option_root:)
      return results unless option_root

      normalized_root = option_root.to_s.upcase
      normalized_ticker = Tickrake::Query::SymbolNormalizer.new.canonical(ticker)
      results.select do |result|
        result.root_symbol.to_s.upcase == normalized_root &&
          result.ticker.to_s.upcase == normalized_ticker
      end
    end

    def select_option_results(results, frequency:, bucket_selector:)
      return results.sort_by { |result| Time.iso8601(result.sample_datetime) } if frequency.nil?

      normalized_frequency = @frequency_normalizer.normalize(frequency)
      bucket_seconds = OPTION_BUCKET_SECONDS.fetch(normalized_frequency) do
        raise Tickrake::Error, "Unsupported option frequency: #{frequency}"
      end

      grouped = results.group_by do |result|
        sampled_at = Time.iso8601(result.sample_datetime).utc.to_i
        [result.provider_name, result.ticker, result.root_symbol, result.expiration_date, sampled_at / bucket_seconds]
      end

      grouped.values.map do |bucket_results|
        bucket_results.sort_by! { |result| Time.iso8601(result.sample_datetime) }
        bucket_selector == :first ? bucket_results.first : bucket_results.last
      end.sort_by { |result| Time.iso8601(result.sample_datetime) }
    end

    def normalize_bucket_selector(value)
      return :last if value.nil?
      return value if %i[first last].include?(value)

      raise Tickrake::Error, "Unsupported bucket selector: #{value}"
    end

    def with_metadata(row, include_metadata:, metadata:)
      return row unless include_metadata

      row.merge("metadata" => metadata)
    end

    def parse_candle_row(row)
      row.to_h.each_with_object({}) do |(key, value), parsed|
        parsed[key] =
          case key
          when "datetime"
            Time.iso8601(value).utc
          else
            coerce_value(key, value, float_fields: CANDLE_FLOAT_FIELDS, integer_fields: CANDLE_INTEGER_FIELDS)
          end
      end
    end

    def parse_option_row(row)
      row.to_h.each_with_object({}) do |(key, value), parsed|
        parsed[key] =
          case key
          when "expiration_date"
            blank?(value) ? nil : Date.iso8601(value)
          else
            coerce_value(key, value, float_fields: OPTION_FLOAT_FIELDS, integer_fields: OPTION_INTEGER_FIELDS)
          end
      end
    end

    def coerce_value(key, value, float_fields:, integer_fields:)
      return nil if blank?(value)
      return value.to_f if float_fields.include?(key)
      return value.to_i if integer_fields.include?(key)

      value
    end

    def blank?(value)
      value.nil? || value.strip.empty?
    end
  end
end
