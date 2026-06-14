#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative "../lib/tickrake"

module Tickrake
  module Scripts
    class Sp500CanonicalDataGenerator
      MEMBERSHIP_HEADERS = %w[index_code canonical_ticker start_date end_date].freeze
      TICKER_HEADERS = %w[
        canonical_ticker
        security_name
        gics_sector
        gics_sub_industry
        headquarters_location
        cik
        founded
        status
      ].freeze
      ALIAS_HEADERS = %w[
        canonical_ticker
        alias_ticker
        start_date
        end_date
        alias_status
        notes
      ].freeze

      class SymbolNormalizer
        def normalize(symbol)
          symbol.to_s.strip.upcase.tr(".", "-")
        end
      end

      def initialize(
        memberships_source:,
        tickers_source:,
        status_source:,
        output_dir:,
        index_code: "SP500",
        symbol_normalizer: SymbolNormalizer.new
      )
        @memberships_source = memberships_source
        @tickers_source = tickers_source
        @status_source = status_source
        @output_dir = output_dir
        @index_code = index_code
        @symbol_normalizer = symbol_normalizer
      end

      def generate!
        status_map = build_status_map
        canonical_memberships = build_memberships(status_map)
        tickers = build_tickers(status_map)
        alias_history = build_alias_history(status_map)

        FileUtils.mkdir_p(@output_dir)
        write_csv(File.join(@output_dir, "market_index_memberships.csv"), MEMBERSHIP_HEADERS, canonical_memberships)
        write_csv(File.join(@output_dir, "tickers.csv"), TICKER_HEADERS, tickers)
        write_csv(File.join(@output_dir, "ticker_alias_history.csv"), ALIAS_HEADERS, alias_history)

        {
          memberships: canonical_memberships,
          tickers: tickers,
          alias_history: alias_history
        }
      end

      private

      def build_status_map
        csv_rows(@status_source).each_with_object({}) do |row, map|
          ticker = normalize_symbol(row.fetch("ticker"))
          successor = normalize_symbol(row["new_ticker"])
          map[ticker] = {
            "ticker" => ticker,
            "first_start" => row["first_start"],
            "last_end" => row["last_end"],
            "status" => row.fetch("status"),
            "new_ticker" => successor
          }
        end
      end

      def build_memberships(status_map)
        grouped = Hash.new { |hash, key| hash[key] = [] }

        csv_rows(@memberships_source).each do |row|
          canonical_ticker = resolve_canonical_ticker!(normalize_symbol(row.fetch("ticker")), status_map)
          grouped[canonical_ticker] << [parse_date!(row.fetch("start_date"), field: "start_date"), parse_optional_date(row["end_date"])]
        end

        grouped.keys.sort.flat_map do |canonical_ticker|
          merge_intervals(grouped.fetch(canonical_ticker)).map do |start_date, end_date|
            {
              "index_code" => @index_code,
              "canonical_ticker" => canonical_ticker,
              "start_date" => start_date.iso8601,
              "end_date" => end_date&.iso8601
            }
          end
        end
      end

      def build_tickers(status_map)
        csv_rows(@tickers_source).map do |row|
          canonical_ticker = resolve_canonical_ticker!(normalize_symbol(row.fetch("Symbol")), status_map)
          status_row = status_map[canonical_ticker]
          raise Tickrake::Error, "Missing status metadata for canonical ticker `#{canonical_ticker}`." unless status_row

          {
            "canonical_ticker" => canonical_ticker,
            "security_name" => row["Security"],
            "gics_sector" => row["GICS Sector"],
            "gics_sub_industry" => row["GICS Sub-Industry"],
            "headquarters_location" => row["Headquarters Location"],
            "cik" => row["CIK"],
            "founded" => row["Founded"],
            "status" => status_row.fetch("status")
          }
        end.sort_by { |row| row.fetch("canonical_ticker") }
      end

      def build_alias_history(status_map)
        status_map.values
          .select { |row| true_rename?(row) }
          .map do |row|
            canonical_ticker = resolve_canonical_ticker!(row.fetch("ticker"), status_map)
            {
              "canonical_ticker" => canonical_ticker,
              "alias_ticker" => row.fetch("ticker"),
              "start_date" => row["first_start"],
              "end_date" => row["last_end"],
              "alias_status" => row.fetch("status"),
              "notes" => "renamed to #{row.fetch("new_ticker")}"
            }
          end
          .sort_by { |row| [row.fetch("canonical_ticker"), row.fetch("start_date"), row.fetch("alias_ticker")] }
      end

      def resolve_canonical_ticker!(ticker, status_map, trail = [])
        row = status_map[ticker]
        return ticker unless row
        return ticker unless true_rename?(row)
        raise Tickrake::Error, "Rename cycle detected: #{(trail + [ticker]).join(' -> ')}" if trail.include?(ticker)

        resolve_canonical_ticker!(row.fetch("new_ticker"), status_map, trail + [ticker])
      end

      def merge_intervals(intervals)
        merged = []
        intervals
          .sort_by { |start_date, end_date| [start_date, end_date || Date.new(9999, 12, 31)] }
          .each do |start_date, end_date|
            if merged.empty?
              merged << [start_date, end_date]
              next
            end

            previous_start, previous_end = merged.last
            if mergeable_intervals?(previous_end, start_date)
              merged[-1] = [previous_start, merge_interval_end(previous_end, end_date)]
            else
              merged << [start_date, end_date]
            end
          end

        merged.each_cons(2) do |left, right|
          left_end = left[1]
          next if left_end.nil?
          next if right[0] > left_end + 1

          raise Tickrake::Error, "Conflicting canonical intervals for #{@index_code} #{left.inspect} and #{right.inspect}."
        end

        merged
      end

      def mergeable_intervals?(previous_end, start_date)
        return true if previous_end.nil?

        start_date <= previous_end + 1
      end

      def merge_interval_end(previous_end, end_date)
        return nil if previous_end.nil? || end_date.nil?
        return previous_end if previous_end >= end_date

        end_date
      end

      def true_rename?(row)
        row.fetch("status") == "renamed" && row["new_ticker"] && row.fetch("ticker") != row.fetch("new_ticker")
      end

      def csv_rows(path)
        CSV.read(path, headers: true, encoding: "bom|utf-8").map(&:to_h)
      end

      def write_csv(path, headers, rows)
        CSV.open(path, "w", write_headers: true, headers: headers, encoding: "utf-8") do |csv|
          rows.each do |row|
            csv << headers.map { |header| row[header] }
          end
        end
      end

      def normalize_symbol(value)
        normalized = @symbol_normalizer.normalize(value)
        normalized.empty? ? nil : normalized
      end

      def parse_date!(value, field:)
        Date.iso8601(value.to_s)
      rescue Date::Error
        raise Tickrake::Error, "Invalid #{field} `#{value}` in #{@memberships_source}."
      end

      def parse_optional_date(value)
        return nil if value.nil? || value.empty?

        Date.iso8601(value)
      rescue Date::Error
        raise Tickrake::Error, "Invalid end_date `#{value}` in #{@memberships_source}."
      end
    end
  end
end

if $PROGRAM_NAME == __FILE__
  root = File.expand_path("..", __dir__)

  Tickrake::Scripts::Sp500CanonicalDataGenerator.new(
    memberships_source: File.join(root, "data", "sp500_ticker_start_end.csv"),
    tickers_source: File.join(root, "data", "sp500.csv"),
    status_source: File.join(root, "data", "sp500_ticker_status_2015_2026.csv"),
    output_dir: File.join(root, "data")
  ).generate!
end
