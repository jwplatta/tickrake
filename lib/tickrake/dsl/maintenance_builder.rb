# frozen_string_literal: true

module Tickrake
  module DSL
    class MaintenanceBuilder
      def initialize
        @tasks = []
        @start_date = nil
        @end_date = nil
      end

      def start_date(date)
        @start_date = date.is_a?(Date) ? date : Date.iso8601(date.to_s)
      end

      def end_date(date)
        @end_date = date.is_a?(Date) ? date : Date.iso8601(date.to_s)
      end

      def compact(subject, universe: nil, universes: [], delete_sources: false)
        @tasks << Tickrake::MaintenanceStepConfig.new(
          action: "compact",
          subject: subject.to_s,
          provider: nil,
          universe: universe&.to_s,
          universes: Array(universes).map(&:to_s),
          tickers: [],
          option_root: nil,
          delete_sources: delete_sources,
          destination: nil,
          artifacts: [],
          retain_local: {}
        )
      end

      def archive(subject, universe: nil, universes: [], to:, artifacts: [], retain: {})
        artifact_strings = Array(artifacts).map(&:to_s)
        retain_strings   = retain.transform_keys(&:to_s)
        retain_local     = artifact_strings.each_with_object({}) do |art, h|
          h[art] = retain_strings.fetch(art, false)
        end

        @tasks << Tickrake::MaintenanceStepConfig.new(
          action: "archive",
          subject: subject.to_s,
          provider: nil,
          universe: universe&.to_s,
          universes: Array(universes).map(&:to_s),
          tickers: [],
          option_root: nil,
          delete_sources: false,
          destination: to.to_s,
          artifacts: artifact_strings,
          retain_local: retain_local
        )
      end

      def build!
        { tasks: @tasks, start_date: @start_date, end_date: @end_date }
      end
    end
  end
end
