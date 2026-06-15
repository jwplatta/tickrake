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

      def initialize(config:, tracker:, symbol_normalizer: SymbolNormalizer.new, frequency_normalizer: FrequencyNormalizer.new,
                     metadata_builder: nil)
        @config = config
        @tracker = tracker
        @symbol_normalizer = symbol_normalizer
        @frequency_normalizer = frequency_normalizer
        @metadata_builder = metadata_builder || CandleMetadata.new(config: config, symbol_normalizer: symbol_normalizer)
      end

      def scan(provider_name: nil, ticker: nil, frequency: nil, start_date: nil, end_date: nil)
        requested_frequency = @frequency_normalizer.normalize(frequency)
        results = build_results(
          metadata_rows(provider_name: provider_name),
          ticker: ticker,
          requested_frequency: requested_frequency,
          start_date: start_date,
          end_date: end_date
        )
        return results unless results.empty?

        seed_metadata(provider_name: provider_name)
        build_results(
          metadata_rows(provider_name: provider_name),
          ticker: ticker,
          requested_frequency: requested_frequency,
          start_date: start_date,
          end_date: end_date
        )
      end

      private

      def metadata_rows(provider_name:)
        where_clauses = ["dataset_type = ?"]
        binds = ["candles"]
        if provider_name
          where_clauses << "provider_name = ?"
          binds << provider_name.to_s
        end
        @tracker.file_metadata_rows(where: where_clauses.join(" AND "), binds: binds)
      end

      def build_results(rows, ticker:, requested_frequency:, start_date:, end_date:)
        rows.filter_map do |metadata|
          selected_provider = metadata.fetch("provider_name")
          selected_definition = @config.provider_definition(selected_provider)
          requested_canonical = ticker && @symbol_normalizer.canonical(ticker, provider_definition: selected_definition)
          next if requested_canonical && metadata.fetch("ticker") != requested_canonical
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
            path: metadata.fetch("path"),
            row_count: metadata.fetch("row_count").to_i,
            first_observed_at: metadata["first_observed_at"],
            last_observed_at: metadata["last_observed_at"],
            coverage: coverage
          )
        end.sort_by { |result| [result.provider_name, result.ticker, result.frequency] }
      end

      def seed_metadata(provider_name:)
        provider_names(provider_name).each do |selected_provider|
          candle_paths_for(selected_provider).each do |path|
            metadata_for(path, provider_name: selected_provider)
          end
        end
      end

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

        metadata = @metadata_builder.build(path: path, provider_name: provider_name)
        return nil unless metadata

        @tracker.upsert_file_metadata(metadata)
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
