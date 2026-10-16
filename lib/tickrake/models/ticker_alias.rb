# frozen_string_literal: true

require_relative "base"

module Tickrake
  module Models
    class TickerAlias < Base
      self.table_name = "ticker_aliases"

      belongs_to :ticker, class_name: "Tickrake::Models::Ticker"
    end
  end
end
