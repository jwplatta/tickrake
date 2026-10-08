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

      def rotation_interval(seconds)
        @rotation_interval_seconds = Integer(seconds)
      end

      def rotation_size(bytes)
        @rotation_size_bytes = Integer(bytes)
      end

      def rotation_size_mb(megabytes)
        @rotation_size_bytes = (Float(megabytes) * 1024 * 1024).round
      end

      def build!(job_name:, inline_symbols:)
        services = @services || []
        raise Tickrake::Error, "level_one job `#{job_name}` requires at least one service" if services.empty?
        raise Tickrake::Error, "level_one job `#{job_name}` requires symbols" if inline_symbols.empty?

        Tickrake::LevelOneConfig.new(
          services: services,
          rotation_interval_seconds: @rotation_interval_seconds || 900,
          rotation_size_bytes: @rotation_size_bytes
        )
      end
    end
  end
end
