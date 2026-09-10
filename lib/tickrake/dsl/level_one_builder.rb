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
        normalized = Array(list).map do |s|
          # Accept both DSL symbols (:level_one_futures) and string constants ("LEVELONE_FUTURES")
          SchwabRb::Stream::Services::SYMBOL_TO_SERVICE.fetch(s.to_sym) { s.to_s.upcase }
        end
        unknown = normalized - VALID_SERVICES
        raise Tickrake::Error, "Unknown level_one services: #{unknown.join(", ")}" unless unknown.empty?

        @services = normalized
      end

      def flush_interval(seconds)
        @flush_interval_seconds = Integer(seconds)
      end

      def retention_days(days)
        @retention_days = Integer(days)
      end

      def build!(job_name:, inline_symbols:)
        services = @services || []
        raise Tickrake::Error, "level_one job `#{job_name}` requires at least one service" if services.empty?
        raise Tickrake::Error, "level_one job `#{job_name}` requires symbols" if inline_symbols.empty?

        Tickrake::LevelOneConfig.new(
          services: services,
          flush_interval_seconds: @flush_interval_seconds || 60,
          retention_days: @retention_days || 30
        )
      end
    end
  end
end
