# frozen_string_literal: true

module Tickrake
  module Query
    class CandlesScanner
      Result = Struct.new(
        :dataset_type,
        :provider_name,
        :ticker,
        :frequency,
        :path,
        :row_count,
        :first_observed_at,
        :last_observed_at,
        :coverage,
        keyword_init: true
      )

      def initialize(config:, tracker:, symbol_normalizer: SymbolNormalizer.new, frequency_normalizer: FrequencyNormalizer.new)
        @config = config
        @tracker = tracker
        @symbol_normalizer = symbol_normalizer
        @frequency_normalizer = frequency_normalizer
      end

      def scan(provider_name: nil, ticker: nil, frequency: nil, start_date: nil, end_date: nil)
        requested_frequency = @frequency_normalizer.normalize(frequency)
        requested_token = ticker && @symbol_normalizer.storage_token(ticker)
        provider_names(provider_name).flat_map do |selected_provider|
          candle_paths_for(selected_provider).filter_map do |path|
            metadata = metadata_for(path, provider_name: selected_provider)
            next unless metadata
            next if requested_token && metadata.fetch("ticker") != requested_token
            next if requested_frequency && metadata.fetch("frequency") != requested_frequency

            coverage = coverage_for(
              start_date: start_date,
              end_date: end_date,
              first_observed_at: metadata["first_observed_at"],
              last_observed_at: metadata["last_observed_at"]
            )
            next if coverage == "none"

            Result.new(
              dataset_type: "candles",
              provider_name: selected_provider,
              ticker: metadata.fetch("ticker"),
              frequency: metadata.fetch("frequency"),
              path: path,
              row_count: metadata.fetch("row_count").to_i,
              first_observed_at: metadata["first_observed_at"],
              last_observed_at: metadata["last_observed_at"],
              coverage: coverage
            )
          end
        end.sort_by { |result| [result.provider_name, result.ticker, result.frequency] }
      end

      private

      def provider_names(provider_name)
        return [provider_name.to_s] if provider_name

        @config.providers.keys.sort
      end

      def candle_paths_for(provider_name)
        base_dir = File.join(@config.history_dir, provider_name.to_s)
        return [] unless Dir.exist?(base_dir)

        Dir.glob(File.join(base_dir, "*.csv")).sort
      end

      def metadata_for(path, provider_name:)
        stat = File.stat(path)
        cached = @tracker.file_metadata(path)
        return cached if cache_hit?(cached, stat)

        basename = File.basename(path, ".csv")
        match = /\A(?<ticker>.+)_(?<frequency>[^_]+)\z/.match(basename)
        return nil unless match

        row_count = 0
        first_observed_at = nil
        last_observed_at = nil

        File.foreach(path).with_index do |line, index|
          next if index.zero?
          next if line.strip.empty?

          row_count += 1
          observed_at = line.split(",", 2).first
          first_observed_at ||= observed_at
          last_observed_at = observed_at
        end

        @tracker.upsert_file_metadata(
          path: path,
          dataset_type: "candles",
          provider_name: provider_name,
          ticker: match[:ticker],
          frequency: match[:frequency],
          row_count: row_count,
          first_observed_at: first_observed_at,
          last_observed_at: last_observed_at,
          file_mtime: stat.mtime.to_i,
          file_size: stat.size,
          updated_at: Time.now
        )
        @tracker.file_metadata(path)
      end

      def cache_hit?(cached, stat)
        cached &&
          cached["file_mtime"].to_i == stat.mtime.to_i &&
          cached["file_size"].to_i == stat.size
      end

      def coverage_for(start_date:, end_date:, first_observed_at:, last_observed_at:)
        return "all" unless start_date || end_date
        return "none" unless first_observed_at && last_observed_at

        first_date = Time.iso8601(first_observed_at).utc.to_date
        last_date = Time.iso8601(last_observed_at).utc.to_date
        requested_start = start_date || first_date
        requested_end = end_date || last_date

        return "none" if last_date < requested_start || first_date > requested_end
        return "full" if first_date <= requested_start && last_date >= requested_end

        "partial"
      end
    end
  end
end
