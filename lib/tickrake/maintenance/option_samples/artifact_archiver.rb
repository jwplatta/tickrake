# frozen_string_literal: true

module Tickrake
  module Maintenance
    module OptionSamples
      class ArtifactArchiver
        def initialize(context:, archive_services: {})
          @context = context
          @archive_services = archive_services
        end

        def upload(destination_name:, artifacts:)
          with_artifacts(destination_name: destination_name, artifacts: artifacts) do |artifact, path, service|
            @context.log(:info, "archive start artifact=#{artifact} destination=#{destination_name} path=#{path}")
            service.upload(path)
            remote_object = service.verify(path)
            verify_size!(path, remote_object)
            @context.log(:info, "archive uploaded artifact=#{artifact} destination=#{destination_name} remote_uri=#{remote_object.uri}")
            { artifact: artifact, path: path, remote_uri: remote_object.uri }
          end
        end

        def verify_existing(destination_name:, artifacts:)
          with_artifacts(destination_name: destination_name, artifacts: artifacts) do |artifact, path, service|
            remote_object = service.verify(path)
            verify_size!(path, remote_object)
            { artifact: artifact, path: path, remote_uri: remote_object.uri }
          end
        end

        private

        def with_artifacts(destination_name:, artifacts:)
          selected_artifacts = artifacts.empty? ? %w[csv parquet] : artifacts
          results = []
          errors = []

          selected_artifacts.each do |artifact|
            path = @context.compacted_path(artifact)
            raise Tickrake::Error, "Compacted artifact not found: #{path}" unless File.exist?(path)

            results << yield(artifact, path, archive_service_for(destination_name))
          rescue StandardError => e
            @context.log(:error, "archive failed artifact=#{artifact}: #{e.class}: #{e.message}")
            errors << e.message
          end

          ArchiveResult.new(
            success: errors.empty?,
            provider_name: @context.provider_name,
            option_root: @context.option_root,
            sample_date: @context.sample_date,
            artifact_results: results,
            errors: errors
          )
        end

        def archive_service_for(destination_name)
          return @archive_services.fetch(destination_name) if @archive_services.key?(destination_name)

          archive_config = @context.config.archives.fetch(destination_name)
          Tickrake::Storage::S3Archive.new(@context.config, archive_config: archive_config)
        end

        def verify_size!(path, remote_object)
          local_size = File.size(path)
          return if remote_object.size == local_size

          raise Tickrake::Error, "Archived object size mismatch for #{path}: local=#{local_size} remote=#{remote_object.size}"
        end
      end
    end
  end
end
