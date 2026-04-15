# frozen_string_literal: true

module Tickrake
  module Query
    class OptionsScanner
      Result = Struct.new(
        :dataset_type,
        :provider_name,
        :ticker,
        :root_symbol,
        :expiration_date,
        :sample_datetime,
        :file_path,
        keyword_init: true
      )

      def initialize(config:, tracker:, symbol_normalizer: SymbolNormalizer.new)
        @config = config
        @tracker = tracker
        @symbol_normalizer = symbol_normalizer
      end

      def scan(provider_name: nil, ticker: nil, start_date: nil, end_date: nil)
        requested_aliases = ticker && requested_aliases_for(ticker)
        provider_names(provider_name).flat_map do |selected_provider|
          snapshot_results_for(
            provider_name: selected_provider,
            requested_aliases: requested_aliases,
            start_date: start_date,
            end_date: end_date
          )
        end.sort_by { |result| [result.provider_name, result.ticker, result.expiration_date, result.sample_datetime] }
      end

      private

      def provider_names(provider_name)
        return [provider_name.to_s] if provider_name

        @config.providers.keys.sort
      end

      def snapshot_results_for(provider_name:, requested_aliases:, start_date:, end_date:)
        option_paths_for(provider_name).filter_map do |path|
          metadata = metadata_for(path, provider_name: provider_name)
          next unless metadata

          canonical_ticker = resolve_canonical_ticker(metadata.fetch("ticker"))
          next if requested_aliases && !requested_aliases.include?(canonical_ticker)

          observed_at = metadata["last_observed_at"]
          next unless within_window?(observed_at, start_date: start_date, end_date: end_date)

          Result.new(
            dataset_type: "options",
            provider_name: provider_name,
            ticker: canonical_ticker,
            root_symbol: resolve_root_symbol(metadata.fetch("ticker")),
            expiration_date: metadata.fetch("expiration_date"),
            sample_datetime: observed_at,
            file_path: path
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
        parsed = parse_path(path)
        return nil unless parsed

        cached = @tracker.file_metadata(path)
        if cache_hit?(cached, stat)
          return cached.merge(
            "expiration_date" => parsed.fetch(:expiration_date)
          )
        end

        sampled_at = parsed.fetch(:sampled_at)
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
          ticker: parsed.fetch(:ticker),
          frequency: nil,
          row_count: 1,
          first_observed_at: observed_at,
          last_observed_at: observed_at,
          file_mtime: stat.mtime.to_i,
          file_size: stat.size,
          updated_at: Time.now
        )
        @tracker.file_metadata(path).merge(
          "expiration_date" => parsed.fetch(:expiration_date)
        )
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

      def resolve_root_symbol(storage_token)
        token = storage_token.to_s.upcase
        matched = @config.options_universe.find do |entry|
          @symbol_normalizer.storage_token(entry.symbol) == token ||
            (entry.option_root && @symbol_normalizer.storage_token(entry.option_root) == token)
        end
        return @symbol_normalizer.canonical(matched.option_root) if matched&.option_root

        token
      end

      def parse_path(path)
        basename = File.basename(path, ".csv")
        match = /\A(?<ticker>.+)_exp(?<expiration_date>\d{4}-\d{2}-\d{2})_(?<sampled_at>\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\z/.match(basename)
        return nil unless match

        {
          ticker: match[:ticker],
          expiration_date: match[:expiration_date],
          sampled_at: DateTime.strptime(match[:sampled_at], "%Y-%m-%d_%H-%M-%S")
        }
      end

      def requested_aliases_for(ticker)
        canonical = @symbol_normalizer.canonical(ticker)
        aliases = [canonical]
        matched = @config.options_universe.find do |entry|
          @symbol_normalizer.same_symbol?(entry.symbol, ticker) ||
            (entry.option_root && @symbol_normalizer.same_symbol?(entry.option_root, ticker))
        end
        if matched
          aliases << @symbol_normalizer.canonical(matched.symbol)
          aliases << @symbol_normalizer.canonical(matched.option_root) if matched.option_root
        end
        aliases.compact.uniq
      end

      def within_window?(observed_at, start_date:, end_date:)
        return true unless start_date || end_date

        observed_date = Time.iso8601(observed_at).utc.to_date
        return false if start_date && observed_date < start_date
        return false if end_date && observed_date > end_date

        true
      end
    end
  end
end
