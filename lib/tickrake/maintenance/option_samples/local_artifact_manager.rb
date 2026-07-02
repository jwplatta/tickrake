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

            if keep_local
              mark_local_and_remote(path: path, remote_uri: remote_uris.fetch(path))
            else
              delete_local_copy(path: path, remote_uri: remote_uris.fetch(path))
            end
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

        private

        def mark_local_and_remote(path:, remote_uri:)
          metadata = fetch_metadata(path)
          @context.tracker.upsert_file_metadata(
            path: path,
            dataset_type: metadata.fetch("dataset_type"),
            provider_name: metadata.fetch("provider_name"),
            ticker: metadata.fetch("ticker"),
            frequency: metadata["frequency"],
            expiration_date: metadata["expiration_date"],
            storage_format: metadata.fetch("storage_format"),
            storage_location: "local",
            artifact_status: "ready_local_and_remote",
            remote_uri: remote_uri,
            source_file_count: metadata["source_file_count"],
            row_count: metadata.fetch("row_count"),
            first_observed_at: metadata["first_observed_at"],
            last_observed_at: metadata["last_observed_at"],
            file_mtime: File.mtime(path).to_i,
            file_size: File.size(path),
            updated_at: Time.now
          )
        end

        def delete_local_copy(path:, remote_uri:)
          metadata = fetch_metadata(path)
          original_mtime = metadata["file_mtime"]
          File.delete(path)
          @context.tracker.upsert_file_metadata(
            path: path,
            dataset_type: metadata.fetch("dataset_type"),
            provider_name: metadata.fetch("provider_name"),
            ticker: metadata.fetch("ticker"),
            frequency: metadata["frequency"],
            expiration_date: metadata["expiration_date"],
            storage_format: metadata.fetch("storage_format"),
            storage_location: "remote",
            artifact_status: "remote",
            remote_uri: remote_uri,
            source_file_count: metadata["source_file_count"],
            row_count: metadata.fetch("row_count"),
            first_observed_at: metadata["first_observed_at"],
            last_observed_at: metadata["last_observed_at"],
            file_mtime: original_mtime,
            file_size: 0,
            updated_at: Time.now
          )
        end

        def fetch_metadata(path)
          metadata = @context.tracker.file_metadata(path)
          raise Tickrake::Error, "Compacted artifact metadata not found: #{path}" unless metadata

          metadata
        end
      end
    end
  end
end
