# frozen_string_literal: true

module Tickrake
  module DSL
    class ChartStreamBuilder
      VALID_SERVICES = %w[CHART_EQUITY CHART_FUTURES].freeze

      def services(list)
        symbols = Array(list).map(&:to_sym)
        unknown = symbols.reject { |s| SchwabRb::Stream::Services::SYMBOL_TO_SERVICE.key?(s) }
        raise Tickrake::Error, "Unknown chart_stream services: #{unknown.join(", ")}" unless unknown.empty?

        @services = symbols
      end

      def flush_interval(seconds)
        @flush_interval_seconds = Integer(seconds)
      end

      def build!(job_name:, inline_symbols:)
        services = @services || []
        raise Tickrake::Error, "chart_stream job `#{job_name}` requires at least one service" if services.empty?
        raise Tickrake::Error, "chart_stream job `#{job_name}` requires symbols" if inline_symbols.empty?

        Tickrake::ChartStreamConfig.new(
          services: services,
          flush_interval_seconds: @flush_interval_seconds || 60
        )
      end
    end
  end
end
