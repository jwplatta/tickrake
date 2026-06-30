# frozen_string_literal: true

module Tickrake
  module Query
    class CompactedOptionsScanner
      Result = Struct.new(
        :dataset_type,
        :provider_name,
        :ticker,
        :sample_date,
        :csv_artifact_status,
        :parquet_artifact_status,
        :csv_remote_uri,
        :parquet_remote_uri,
        :archive_state,
        keyword_init: true
      )

      def initialize(config:, tracker:, symbol_normalizer: SymbolNormalizer.new)
        @config = config
        @tracker = tracker
        @symbol_normalizer = symbol_normalizer
      end

      def scan(provider_name: nil, ticker: nil, start_date: nil, end_date: nil, limit: nil, ascending: true)
        requested_tickers = ticker && requested_storage_tokens_for(ticker)
        grouped_rows = @tracker.file_metadata_rows(
          where: where_clause(provider_name: provider_name, requested_tickers: requested_tickers),
          binds: binds(provider_name: provider_name, requested_tickers: requested_tickers),
          order_by: "path ASC"
        ).each_with_object({}) do |row, memo|
          sample_date = extract_sample_date(row.fetch("path"))
          next unless sample_date
          next if start_date && sample_date < start_date
          next if end_date && sample_date > end_date

          key = [row.fetch("provider_name"), row.fetch("ticker"), sample_date]
          memo[key] ||= []
          memo[key] << row
        end

        results = grouped_rows.map do |(result_provider_name, storage_ticker, sample_date), rows|
          csv_row = rows.find { |row| row.fetch("dataset_type") == "options_compacted_csv" }
          parquet_row = rows.find { |row| row.fetch("dataset_type") == "options_compacted_parquet" }

          csv_remote_uri = csv_row && csv_row["remote_uri"]
          parquet_remote_uri = parquet_row && parquet_row["remote_uri"]

          Result.new(
            dataset_type: "compacted-options",
            provider_name: result_provider_name,
            ticker: resolve_root_symbol(storage_ticker),
            sample_date: sample_date,
            csv_artifact_status: csv_row && csv_row["artifact_status"],
            parquet_artifact_status: parquet_row && parquet_row["artifact_status"],
            csv_remote_uri: csv_remote_uri,
            parquet_remote_uri: parquet_remote_uri,
            archive_state: archive_state(csv_remote_uri, parquet_remote_uri)
          )
        end

        ordered = results.sort_by { |result| [result.sample_date, result.provider_name, result.ticker] }
        ordered.reverse! unless ascending
        limit ? ordered.first(limit) : ordered
      end

      private

      def where_clause(provider_name:, requested_tickers:)
        clauses = ["dataset_type IN (?, ?)"]
        clauses << "provider_name = ?" if provider_name
        if requested_tickers
          placeholders = Array.new(requested_tickers.length, "?").join(", ")
          clauses << "ticker IN (#{placeholders})"
        end
        clauses.join(" AND ")
      end

      def binds(provider_name:, requested_tickers:)
        values = ["options_compacted_csv", "options_compacted_parquet"]
        values << provider_name.to_s if provider_name
        values.concat(requested_tickers) if requested_tickers
        values
      end

      def extract_sample_date(path)
        match = path.match(/_samples_(\d{4}-\d{2}-\d{2})\.(csv|parquet)\z/)
        return nil unless match

        Date.iso8601(match[1])
      end

      def archive_state(csv_remote_uri, parquet_remote_uri)
        csv_archived = !csv_remote_uri.to_s.empty?
        parquet_archived = !parquet_remote_uri.to_s.empty?
        return "archived" if csv_archived && parquet_archived
        return "partial" if csv_archived || parquet_archived

        "local_only"
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

      def resolve_root_symbol(storage_token)
        token = storage_token.to_s.upcase
        matched = @config.options_universe.find do |entry|
          @symbol_normalizer.storage_token(entry.symbol) == token ||
            (entry.option_root && @symbol_normalizer.storage_token(entry.option_root) == token)
        end
        return @symbol_normalizer.canonical(matched.option_root) if matched&.option_root

        token
      end
    end
  end
end
