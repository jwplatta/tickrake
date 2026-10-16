# frozen_string_literal: true

require_relative "base"

module Tickrake
  module Models
    class MarketIndexMembership < Base
      self.table_name = "market_index_memberships"

      belongs_to :market_index, class_name: "Tickrake::Models::MarketIndex"
      belongs_to :ticker, class_name: "Tickrake::Models::Ticker"
    end
  end
end
