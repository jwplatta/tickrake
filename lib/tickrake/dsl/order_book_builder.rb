# frozen_string_literal: true

module Tickrake
  module DSL
    class OrderBookBuilder
      EQUITY_SERVICES  = %w[NYSE_BOOK NASDAQ_BOOK].freeze
      OPTIONS_SERVICES = %w[OPTIONS_BOOK].freeze
      ALL_SERVICES     = (EQUITY_SERVICES + OPTIONS_SERVICES).freeze

      def services(list)
        normalized = Array(list).map { |s| s.to_s.upcase }
        unknown = normalized - ALL_SERVICES
        raise Tickrake::Error, "Unknown order_book services: #{unknown.join(", ")}" unless unknown.empty?

        @services = normalized
      end

      def flush_interval(seconds)
        @flush_interval_seconds = Integer(seconds)
      end

      def retention_days(days)
        @retention_days = Integer(days)
      end

      def contracts(&block)
        @contracts_builder = OrderBookContractsBuilder.new
        @contracts_builder.instance_eval(&block)
      end

      def build!(job_name:, inline_symbols:)
        services = @services || []
        raise Tickrake::Error, "order_book job `#{job_name}` requires at least one service" if services.empty?

        has_options = services.any? { |s| OPTIONS_SERVICES.include?(s) }
        has_equity  = services.any? { |s| EQUITY_SERVICES.include?(s) }

        if has_options && has_equity
          raise Tickrake::Error,
                "order_book job `#{job_name}` cannot mix equity book services with OPTIONS_BOOK in the same job"
        end

        contracts = @contracts_builder&.build!

        if has_options && contracts.nil?
          raise Tickrake::Error,
                "order_book job `#{job_name}` with OPTIONS_BOOK requires a contracts block"
        end

        if has_equity && inline_symbols.empty?
          raise Tickrake::Error,
                "order_book job `#{job_name}` with equity book services requires top-level symbols"
        end

        Tickrake::OrderBookConfig.new(
          services: services,
          flush_interval_seconds: @flush_interval_seconds || 60,
          retention_days: @retention_days || 30,
          contracts: contracts
        )
      end
    end
  end
end
