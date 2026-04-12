# frozen_string_literal: true

module Tickrake
  module Query
    class OptionsScanner
      Result = Struct.new(
        :dataset_type,
        :provider_name,
        :ticker,
        :snapshot_count,
        :first_observed_at,
        :last_observed_at,
        :latest_path,
        :coverage,
        keyword_init: true
      )

      def initialize(config:, tracker:, symbol_normalizer: SymbolNormalizer.new)
        @config = config
        @tracker = tracker
        @symbol_normalizer = symbol_normalizer
      end

      def scan(provider_name: nil, ticker: nil, start_date: nil, end_date: nil)
        requested_canonical = ticker && @symbol_normalizer.canonical(ticker)
        provider_names(provider_name).flat_map do |selected_provider|
          grouped_results_for(
            provider_name: selected_provider,
            requested_canonical: requested_canonical,
            start_date: start_date,
            end_date: end_date
          )
        end.sort_by { |result| [result.provider_name, result.ticker] }
      end

      private

      def provider_names(provider_name)
        return [provider_name.to_s] if provider_name

        @config.providers.keys.sort
      end

      def grouped_results_for(provider_name:, requested_canonical:, start_date:, end_date:)
        grouped = Hash.new { |hash, key| hash[key] = [] }

        option_paths_for(provider_name).each do |path|
          metadata = metadata_for(path, provider_name: provider_name)
          next unless metadata

          canonical_ticker = resolve_canonical_ticker(metadata.fetch("ticker"))
          next if requested_canonical && canonical_ticker != requested_canonical

          observed_at = metadata["last_observed_at"]
          next unless within_window?(observed_at, start_date: start_date, end_date: end_date)

          grouped[canonical_ticker] << {
            path: path,
            observed_at: observed_at
          }
        end

        grouped.map do |canonical_ticker, samples|
          sorted = samples.sort_by { |sample| sample.fetch(:observed_at) }
          Result.new(
            dataset_type: "options",
            provider_name: provider_name,
            ticker: canonical_ticker,
            snapshot_count: samples.length,
            first_observed_at: sorted.first.fetch(:observed_at),
            last_observed_at: sorted.last.fetch(:observed_at),
            latest_path: sorted.last.fetch(:path),
            coverage: coverage_for(start_date: start_date, end_date: end_date, samples: sorted)
          )
        end
      end

      def option_paths_for(provider_name)
        base_dir = File.join(@config.options_dir, provider_name.to_s)
        return [] unless Dir.exist?(base_dir)

        Dir.glob(File.join(base_dir, "*.csv")).sort
      end

      def metadata_for(path, provider_name:)
        stat = File.stat(path)
        cached = @tracker.file_metadata(path)
        return cached if cache_hit?(cached, stat)

        basename = File.basename(path, ".csv")
        match = /\A(?<ticker>.+)_exp\d{4}-\d{2}-\d{2}_(?<sampled_at>\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\z/.match(basename)
        return nil unless match

        sampled_at = DateTime.strptime(match[:sampled_at], "%Y-%m-%d_%H-%M-%S")
        observed_at = Time.utc(
          sampled_at.year,
          sampled_at.month,
          sampled_at.day,
          sampled_at.hour,
          sampled_at.min,
          sampled_at.sec
        ).iso8601
        @tracker.upsert_file_metadata(
          path: path,
          dataset_type: "options",
          provider_name: provider_name,
          ticker: match[:ticker],
          frequency: nil,
          row_count: 1,
          first_observed_at: observed_at,
          last_observed_at: observed_at,
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

      def resolve_canonical_ticker(storage_token)
        token = storage_token.to_s.upcase
        matched = @config.options_universe.find do |entry|
          @symbol_normalizer.storage_token(entry.symbol) == token ||
            (entry.option_root && @symbol_normalizer.storage_token(entry.option_root) == token)
        end
        return @symbol_normalizer.canonical(matched.symbol) if matched

        token
      end

      def within_window?(observed_at, start_date:, end_date:)
        return true unless start_date || end_date

        observed_date = Time.iso8601(observed_at).utc.to_date
        return false if start_date && observed_date < start_date
        return false if end_date && observed_date > end_date

        true
      end

      def coverage_for(start_date:, end_date:, samples:)
        return "all" unless start_date || end_date

        first_date = Time.iso8601(samples.first.fetch(:observed_at)).utc.to_date
        last_date = Time.iso8601(samples.last.fetch(:observed_at)).utc.to_date
        requested_start = start_date || first_date
        requested_end = end_date || last_date

        return "full" if first_date <= requested_start && last_date >= requested_end

        "partial"
      end
    end
  end
end
