# frozen_string_literal: true

module Tickrake
  module OrderBook
    class ContractResolver
      def initialize(client, contracts_config)
        @client = client
        @underlying = contracts_config.underlying
        @atm_strikes = contracts_config.atm_strikes
        @front_n = contracts_config.front_n
      end

      def resolve
        chain = @client.get_option_chain(
          @underlying,
          strike_count: @atm_strikes,
          include_underlying_quote: true
        )
        expirations = chain.option_expiration_list
                           .sort_by(&:expiration_date)
                           .first(@front_n)
        expirations.flat_map { |exp| exp.calls.map(&:symbol) + exp.puts.map(&:symbol) }
      end
    end
  end
end
