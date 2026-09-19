# frozen_string_literal: true

require "parquet"

module Tickrake
  module Storage
    class FundamentalsParquetWriter
      SCHEMA = [
        { "symbol"                   => "string" },
        { "sample_date"              => "string" },
        { "asset_type"               => "string" },
        { "exchange"                 => "string" },
        { "description"              => "string" },
        { "pe_ratio"                 => "double" },
        { "peg_ratio"                => "double" },
        { "pb_ratio"                 => "double" },
        { "pr_ratio"                 => "double" },
        { "pcf_ratio"                => "double" },
        { "gross_margin_ttm"         => "double" },
        { "gross_margin_mrq"         => "double" },
        { "net_profit_margin_ttm"    => "double" },
        { "net_profit_margin_mrq"    => "double" },
        { "operating_margin_ttm"     => "double" },
        { "operating_margin_mrq"     => "double" },
        { "return_on_equity"         => "double" },
        { "return_on_assets"         => "double" },
        { "return_on_investment"     => "double" },
        { "quick_ratio"              => "double" },
        { "current_ratio"            => "double" },
        { "interest_coverage"        => "double" },
        { "total_debt_to_capital"    => "double" },
        { "lt_debt_to_equity"        => "double" },
        { "total_debt_to_equity"     => "double" },
        { "eps_ttm"                  => "double" },
        { "eps_change_percent_ttm"   => "double" },
        { "eps"                      => "double" },
        { "rev_change_year"          => "double" },
        { "rev_change_ttm"           => "double" },
        { "shares_outstanding"       => "double" },
        { "market_cap_float"         => "double" },
        { "market_cap"               => "double" },
        { "book_value_per_share"     => "double" },
        { "short_int_to_float"       => "double" },
        { "short_int_day_to_cover"   => "double" },
        { "dividend_amount"          => "double" },
        { "dividend_yield"           => "double" },
        { "dividend_date"            => "string" },
        { "dividend_pay_date"        => "string" },
        { "dividend_pay_amount"      => "double" },
        { "div_growth_rate_3year"    => "double" },
        { "dividend_freq"            => "int64" },
        { "beta"                     => "double" },
        { "high_52"                  => "double" },
        { "low_52"                   => "double" },
        { "avg_10_days_volume"       => "int64" },
        { "avg_1_day_volume"         => "int64" },
        { "avg_3_month_volume"       => "int64" },
        { "avg_1_year_volume"        => "int64" },
        { "last_earnings_date"       => "string" },
        { "fetched_at"               => "string" }
      ].freeze

      def write(path, rows:)
        FileUtils.mkdir_p(File.dirname(path))
        tmp_path = "#{path}.tmp"

        Parquet.write_rows(
          typed_rows(rows),
          schema: SCHEMA,
          write_to: tmp_path
        )
        File.rename(tmp_path, path)
        path
      ensure
        File.delete(tmp_path) if defined?(tmp_path) && tmp_path && File.exist?(tmp_path)
      end

      private

      def typed_rows(rows)
        rows.map do |row|
          [
            row[:symbol]&.to_s,
            row[:sample_date]&.to_s,
            row[:asset_type]&.to_s,
            row[:exchange]&.to_s,
            row[:description]&.to_s,
            row[:pe_ratio]&.to_f,
            row[:peg_ratio]&.to_f,
            row[:pb_ratio]&.to_f,
            row[:pr_ratio]&.to_f,
            row[:pcf_ratio]&.to_f,
            row[:gross_margin_ttm]&.to_f,
            row[:gross_margin_mrq]&.to_f,
            row[:net_profit_margin_ttm]&.to_f,
            row[:net_profit_margin_mrq]&.to_f,
            row[:operating_margin_ttm]&.to_f,
            row[:operating_margin_mrq]&.to_f,
            row[:return_on_equity]&.to_f,
            row[:return_on_assets]&.to_f,
            row[:return_on_investment]&.to_f,
            row[:quick_ratio]&.to_f,
            row[:current_ratio]&.to_f,
            row[:interest_coverage]&.to_f,
            row[:total_debt_to_capital]&.to_f,
            row[:lt_debt_to_equity]&.to_f,
            row[:total_debt_to_equity]&.to_f,
            row[:eps_ttm]&.to_f,
            row[:eps_change_percent_ttm]&.to_f,
            row[:eps]&.to_f,
            row[:rev_change_year]&.to_f,
            row[:rev_change_ttm]&.to_f,
            row[:shares_outstanding]&.to_f,
            row[:market_cap_float]&.to_f,
            row[:market_cap]&.to_f,
            row[:book_value_per_share]&.to_f,
            row[:short_int_to_float]&.to_f,
            row[:short_int_day_to_cover]&.to_f,
            row[:dividend_amount]&.to_f,
            row[:dividend_yield]&.to_f,
            row[:dividend_date]&.to_s,
            row[:dividend_pay_date]&.to_s,
            row[:dividend_pay_amount]&.to_f,
            row[:div_growth_rate_3year]&.to_f,
            row[:dividend_freq]&.to_i,
            row[:beta]&.to_f,
            row[:high_52]&.to_f,
            row[:low_52]&.to_f,
            row[:avg_10_days_volume]&.to_i,
            row[:avg_1_day_volume]&.to_i,
            row[:avg_3_month_volume]&.to_i,
            row[:avg_1_year_volume]&.to_i,
            row[:last_earnings_date]&.to_s,
            row[:fetched_at]&.to_s
          ]
        end
      end
    end
  end
end
