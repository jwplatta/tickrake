# frozen_string_literal: true

require_relative "base"

module Tickrake
  module Models
    class Ticker < Base
      self.table_name = "tickers"

      has_many :ticker_aliases, class_name: "Tickrake::Models::TickerAlias", dependent: :destroy
      has_many :memberships, class_name: "Tickrake::Models::MarketIndexMembership", dependent: :destroy
      has_many :market_indexes, through: :memberships
    end
  end
end
