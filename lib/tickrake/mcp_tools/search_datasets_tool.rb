# frozen_string_literal: true

require "date"
require "json"
require "mcp"

module Tickrake
  module MCPTools
    class SearchDatasetsTool < MCP::Tool
      description "Search stored Tickrake candle files and option snapshots and return dataset metadata only."

      input_schema(
        properties: {
          config_path: {
            type: "string",
            description: "Optional path to a Tickrake config file."
          },
          limit: {
            type: "integer",
            description: "Maximum number of option snapshot records to return. Defaults to 100 for option searches."
          },
          type: {
            type: "string",
            description: "Optional dataset type filter.",
            enum: %w[candles options]
          },
          provider: {
            type: "string",
            description: "Optional configured provider name."
          },
          ticker: {
            type: "string",
            description: "Optional ticker filter."
          },
          frequency: {
            type: "string",
            description: "Optional candle frequency filter.",
            enum: %w[all 1min 5min 30min day]
          },
          start_date: {
            type: "string",
            description: "Optional inclusive start date in YYYY-MM-DD."
          },
          end_date: {
            type: "string",
            description: "Optional inclusive end date in YYYY-MM-DD."
          }
        },
        required: []
      )

      annotations(
        title: "Search Tickrake Datasets",
        read_only_hint: true,
        destructive_hint: false,
        idempotent_hint: true
      )

      class << self
        def call(config_path: nil, type: nil, provider: nil, ticker: nil, frequency: nil,
                 start_date: nil, end_date: nil, limit: 100, server_context:)
          config = Tickrake::ConfigLoader.load(config_path || Tickrake::PathSupport.config_path)
          tracker = Tickrake::Tracker.new(config.sqlite_path)
          limit = normalize_limit(limit)
          scanner_frequency = normalize_frequency_filter(frequency)
          filters = {
            type: type,
            provider: provider,
            ticker: ticker,
            frequency: frequency,
            start_date: parse_date(start_date),
            end_date: parse_date(end_date)
          }

          results = []
          if type.nil? || type == "candles"
            results.concat(
              Tickrake::Query::CandlesScanner.new(config: config, tracker: tracker).scan(
                provider_name: provider,
                ticker: ticker,
                frequency: scanner_frequency,
                start_date: filters[:start_date],
                end_date: filters[:end_date]
              )
            )
          end
          if type.nil? || type == "options"
            results.concat(
              Tickrake::Query::OptionsScanner.new(config: config, tracker: tracker).scan(
                provider_name: provider,
                ticker: ticker,
                start_date: filters[:start_date],
                end_date: filters[:end_date]
              )
            )
          end

          apply_limit = apply_limit?(type, results)
          filters[:limit] = limit if apply_limit
          total_count = results.length
          results = limit_results(results, limit: limit, type: type) if apply_limit

          Response.text(JSON.pretty_generate(
            filters: serialize(filters),
            returned_count: results.length,
            result_count: total_count,
            results: results.map { |result| serialize(result.to_h) }
          ))
        end

        private

        def parse_date(value)
          return nil if value.nil? || value.empty?

          Date.iso8601(value)
        end

        def normalize_frequency_filter(value)
          return nil if value.nil?

          normalized = value.to_s.strip.downcase
          return nil if normalized.empty? || normalized == "all"

          value
        end

        def normalize_limit(value)
          parsed = Integer(value || 100)
          raise Tickrake::Error, "limit must be positive." if parsed <= 0

          parsed
        end

        def apply_limit?(type, results)
          return false if type == "candles"
          return true if type == "options"

          results.any? { |result| result.dataset_type == "options" }
        end

        def limit_results(results, limit:, type:)
          ordered = if type == "options" || results.all? { |result| result.dataset_type == "options" }
            results.sort_by do |result|
              timestamp = result.respond_to?(:sample_datetime) ? result.sample_datetime : nil
              timestamp ? Time.iso8601(timestamp) : Time.at(0)
            end.reverse
          else
            results
          end

          ordered.first(limit)
        end

        def serialize(hash)
          hash.transform_values { |value| value.respond_to?(:iso8601) ? value.iso8601 : value }
        end
      end
    end
  end
end
