# frozen_string_literal: true

module Tickrake
  module DSL
    class LevelOneBuilder
      VALID_SERVICES = %w[
        LEVELONE_EQUITIES
        LEVELONE_OPTIONS
        LEVELONE_FUTURES
        LEVELONE_FUTURES_OPTIONS
        LEVELONE_FOREX
      ].freeze

      def services(list)
        symbols = Array(list).map(&:to_sym)
        unknown = symbols.reject { |s| SchwabRb::Stream::Services::SYMBOL_TO_SERVICE.key?(s) }
        raise Tickrake::Error, "Unknown level_one services: #{unknown.join(", ")}" unless unknown.empty?

        @services = symbols
      end

      def contracts(&block)
        @contracts_builder = OrderBookContractsBuilder.new
        @contracts_builder.instance_eval(&block)
      end

      def build!(job_name:, inline_symbols:)
        services = @services || []
        raise Tickrake::Error, "level_one job `#{job_name}` requires at least one service" if services.empty?

        contracts = @contracts_builder&.build!
        if inline_symbols.empty? && contracts.nil?
          raise Tickrake::Error, "level_one job `#{job_name}` requires symbols or a contracts block"
        end

        Tickrake::LevelOneConfig.new(
          services: services,
          contracts: contracts
        )
      end
    end
  end
end
