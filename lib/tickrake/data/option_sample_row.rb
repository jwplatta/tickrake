# frozen_string_literal: true

module Tickrake
  module Data
    OptionSampleRow = Struct.new(
      :contract_type,
      :symbol,
      :description,
      :strike,
      :expiration_date,
      :open,
      :high,
      :low,
      :close,
      :mark,
      :bid,
      :bid_size,
      :ask,
      :ask_size,
      :last,
      :last_size,
      :open_interest,
      :total_volume,
      :transactions,
      :delta,
      :gamma,
      :theta,
      :vega,
      :rho,
      :volatility,
      :theoretical_volatility,
      :theoretical_option_value,
      :intrinsic_value,
      :extrinsic_value,
      :underlying_price,
      :source,
      :fetched_at,
      keyword_init: true
    )
  end
end
