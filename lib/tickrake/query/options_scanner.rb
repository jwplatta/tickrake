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

      def scan(provider_name: nil, ticker: nil, start_date: nil, end_date: nil, expiration_date: nil,
               limit: nil, ascending: true)
        requested_aliases = ticker && requested_aliases_for(ticker)
        results = snapshot_results_for(
          provider_name: provider_name,
          requested_aliases: requested_aliases,
          start_date: start_date,
          end_date: end_date,
          expiration_date: expiration_date
        )
        if results.empty?
          seed_metadata(provider_name: provider_name)
          results = snapshot_results_for(
            provider_name: provider_name,
            requested_aliases: requested_aliases,
            start_date: start_date,
            end_date: end_date,
            expiration_date: expiration_date
          )
        end

        ordered = results.sort_by { |result| [Time.iso8601(result.sample_datetime), result.provider_name, result.ticker, result.expiration_date] }
        ordered.reverse! unless ascending
        limit ? ordered.first(limit) : ordered
      end

      private

      def snapshot_results_for(provider_name:, requested_aliases:, start_date:, end_date:, expiration_date:)
        where_clauses = ["dataset_type = ?"]
        binds = ["options"]
        if provider_name
          where_clauses << "provider_name = ?"
          binds << provider_name.to_s
        end
        if expiration_date
          where_clauses << "expiration_date = ?"
          binds << expiration_date.iso8601
        end

        @tracker.file_metadata_rows(where: where_clauses.join(" AND "), binds: binds).filter_map do |metadata|
          canonical_ticker = resolve_canonical_ticker(metadata.fetch("ticker"))
          next if requested_aliases && !requested_aliases.include?(canonical_ticker)

          observed_at = metadata["last_observed_at"]
          next unless within_window?(observed_at, start_date: start_date, end_date: end_date)

          Result.new(
            dataset_type: "options",
            provider_name: metadata.fetch("provider_name"),
            ticker: canonical_ticker,
            root_symbol: resolve_root_symbol(metadata.fetch("ticker")),
            expiration_date: metadata.fetch("expiration_date"),
            sample_datetime: observed_at,
            file_path: metadata.fetch("path")
          )
        end
      end

      def seed_metadata(provider_name:)
        provider_names(provider_name).each do |selected_provider|
          option_paths_for(selected_provider).each do |path|
            metadata_for(path, provider_name: selected_provider)
          end
        end
      end

      def provider_names(provider_name)
        return [provider_name.to_s] if provider_name

        @config.providers.keys.sort
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
          expiration_date: parsed.fetch(:expiration_date),
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
