# frozen_string_literal: true

module Tickrake
  module DSL
    class MaintenanceBuilder
      def initialize
        @tasks = []
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
        @tasks
      end
    end
  end
end
