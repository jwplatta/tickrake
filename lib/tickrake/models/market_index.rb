# frozen_string_literal: true

require_relative "base"

module Tickrake
  module Models
    class MarketIndex < Base
      self.table_name = "market_indexes"

      has_many :memberships, class_name: "Tickrake::Models::MarketIndexMembership", dependent: :destroy
      has_many :tickers, through: :memberships
    end
  end
end
