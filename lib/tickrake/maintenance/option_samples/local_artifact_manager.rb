# frozen_string_literal: true

module Tickrake
  module Maintenance
    module OptionSamples
      class LocalArtifactManager
        DEFAULT_RETAIN_LOCAL = { "csv" => false, "parquet" => true }.freeze

        def initialize(context:)
          @context = context
        end

        def apply(remote_uris:, retain_local:, artifacts:, dry_run: false)
          selected_artifacts = artifacts.empty? ? %w[csv parquet] : artifacts
          retained_local = {}

          selected_artifacts.each do |artifact|
            path = @context.compacted_path(artifact)
            keep_local = retain_local.fetch(artifact, DEFAULT_RETAIN_LOCAL.fetch(artifact))
            retained_local[artifact] = keep_local
            next if dry_run

            File.delete(path) unless keep_local
          end

          RetentionResult.new(
            success: true,
            provider_name: @context.provider_name,
            option_root: @context.option_root,
            sample_date: @context.sample_date,
            retained_local: retained_local,
            errors: []
          )
        rescue StandardError => e
          RetentionResult.new(
            success: false,
            provider_name: @context.provider_name,
            option_root: @context.option_root,
            sample_date: @context.sample_date,
            retained_local: retained_local || {},
            errors: [e.message]
          )
        end
      end
    end
  end
end
