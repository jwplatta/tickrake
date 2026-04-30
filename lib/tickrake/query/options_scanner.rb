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
        requested_tickers = ticker && requested_storage_tokens_for(ticker)
        snapshot_results_for(
          provider_name: provider_name,
          requested_tickers: requested_tickers,
          start_date: start_date,
          end_date: end_date,
          expiration_date: expiration_date,
          limit: limit,
          ascending: ascending
        )
      end

      private

      def snapshot_results_for(provider_name:, requested_tickers:, start_date:, end_date:, expiration_date:, limit:, ascending:)
        where_clauses = ["dataset_type = ?"]
        binds = ["options"]
        if provider_name
          where_clauses << "provider_name = ?"
          binds << provider_name.to_s
        end
        if requested_tickers
          placeholders = Array.new(requested_tickers.length, "?").join(", ")
          where_clauses << "ticker IN (#{placeholders})"
          binds.concat(requested_tickers)
        end
        if expiration_date
          where_clauses << "expiration_date = ?"
          binds << expiration_date.iso8601
        end
        if start_date
          where_clauses << "last_observed_at >= ?"
          binds << observed_at_lower_bound(start_date)
        end
        if end_date
          where_clauses << "last_observed_at < ?"
          binds << observed_at_upper_bound(end_date)
        end

        order_direction = ascending ? "ASC" : "DESC"
        order_by = [
          "last_observed_at #{order_direction}",
          "provider_name #{order_direction}",
          "ticker #{order_direction}",
          "expiration_date #{order_direction}"
        ].join(", ")

        @tracker.file_metadata_rows(
          where: where_clauses.join(" AND "),
          binds: binds,
          order_by: order_by,
          limit: limit
        ).map do |metadata|
          canonical_ticker = resolve_canonical_ticker(metadata.fetch("ticker"))
          Result.new(
            dataset_type: "options",
            provider_name: metadata.fetch("provider_name"),
            ticker: canonical_ticker,
            root_symbol: resolve_root_symbol(metadata.fetch("ticker")),
            expiration_date: metadata.fetch("expiration_date"),
            sample_datetime: metadata.fetch("last_observed_at"),
            file_path: metadata.fetch("path")
          )
        end
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

      def requested_storage_tokens_for(ticker)
        storage_tokens = [@symbol_normalizer.storage_token(ticker)]
        matched = @config.options_universe.find do |entry|
          @symbol_normalizer.same_symbol?(entry.symbol, ticker) ||
            (entry.option_root && @symbol_normalizer.same_symbol?(entry.option_root, ticker))
        end
        if matched
          storage_tokens << @symbol_normalizer.storage_token(matched.symbol)
          storage_tokens << @symbol_normalizer.storage_token(matched.option_root) if matched.option_root
        end
        storage_tokens.compact.uniq
      end

      def observed_at_lower_bound(date)
        Time.utc(date.year, date.month, date.day).iso8601
      end

      def observed_at_upper_bound(date)
        next_day = date + 1
        Time.utc(next_day.year, next_day.month, next_day.day).iso8601
      end
    end
  end
end
