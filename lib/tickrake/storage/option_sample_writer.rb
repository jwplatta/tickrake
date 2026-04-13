# frozen_string_literal: true

module Tickrake
  module Storage
    class OptionSampleWriter
      CSV_HEADERS = %w[
        contract_type
        symbol
        description
        strike
        expiration_date
        mark
        bid
        bid_size
        ask
        ask_size
        last
        last_size
        open_interest
        total_volume
        delta
        gamma
        theta
        vega
        rho
        volatility
        theoretical_volatility
        theoretical_option_value
        intrinsic_value
        extrinsic_value
        underlying_price
      ].freeze

      def initialize(csv_writer: Tickrake::Storage::CsvWriter.new)
        @csv_writer = csv_writer
      end

      def write(path:, rows:)
        @csv_writer.write(
          path,
          headers: CSV_HEADERS,
          rows: rows.map { |row| csv_row(row) }
        )
      end

      private

      def csv_row(row)
        [
          row.contract_type,
          row.symbol,
          row.description,
          row.strike,
          row.expiration_date,
          row.mark,
          row.bid,
          row.bid_size,
          row.ask,
          row.ask_size,
          row.last,
          row.last_size,
          row.open_interest,
          row.total_volume,
          row.delta,
          row.gamma,
          row.theta,
          row.vega,
          row.rho,
          row.volatility,
          row.theoretical_volatility,
          row.theoretical_option_value,
          row.intrinsic_value,
          row.extrinsic_value,
          row.underlying_price
        ]
      end
    end
  end
end
