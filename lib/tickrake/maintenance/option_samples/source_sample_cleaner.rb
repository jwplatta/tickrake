# frozen_string_literal: true

module Tickrake
  module Maintenance
    module OptionSamples
      class SourceSampleCleaner
        def initialize(context:, manifest_writer: nil, s3_archive: nil)
          @context = context
          @manifest_writer = manifest_writer
          @s3_archive = s3_archive
        end

        def run(source_paths:, dry_run: false)
          return build_result(source_paths: source_paths, deleted_source_paths: [], errors: []) if dry_run

          if @manifest_writer
            guard_manifest!(
              dataset_type: "options",
              provider: @context.provider_name,
              root: @context.option_root,
              sample_date: @context.sample_date
            )
          end

          deleted_source_paths = []
          errors = []
          source_paths.each do |path|
            File.delete(path)
            deleted_source_paths << path
          rescue StandardError => e
            errors << "Failed to delete source snapshot CSV #{path}: #{e.message}"
            break
          end

          build_result(source_paths: source_paths, deleted_source_paths: deleted_source_paths, errors: errors)
        rescue StandardError => e
          build_result(source_paths: source_paths, deleted_source_paths: [], errors: [e.message])
        end

        private

        def guard_manifest!(dataset_type:, provider:, root:, sample_date:)
          unless @manifest_writer.manifest_exists?(dataset_type: dataset_type, provider: provider, root: root, sample_date: sample_date)
            raise RuntimeError, "Cannot delete source files: manifest does not exist for #{dataset_type}/#{provider}/#{root} #{sample_date}"
          end

          manifest = @manifest_writer.read(dataset_type: dataset_type, provider: provider, root: root, sample_date: sample_date)

          artifacts = manifest["artifacts"]
          raise RuntimeError, "Cannot delete source files: manifest has no artifacts for #{provider}/#{root} #{sample_date}" if artifacts.nil? || artifacts.empty?

          artifacts.each do |format, artifact|
            uri = artifact["uri"]
            key = uri_to_key(uri)
            unless @s3_archive.object_exists?(key)
              raise RuntimeError, "Cannot delete source files: artifact #{format} not accessible at #{uri} for #{provider}/#{root} #{sample_date}"
            end

            row_count = artifact["row_count"]
            unless row_count && row_count > 0
              raise RuntimeError, "Cannot delete source files: artifact #{format} has invalid row_count=#{row_count.inspect} for #{provider}/#{root} #{sample_date}"
            end
          end
        end

        def uri_to_key(uri)
          # Strip s3://bucket/ prefix to get the S3 key
          uri.sub(%r{\As3://[^/]+/}, "")
        end

        def build_result(source_paths:, deleted_source_paths:, errors:)
          SourceCleanupResult.new(
            success: errors.empty?,
            provider_name: @context.provider_name,
            option_root: @context.option_root,
            sample_date: @context.sample_date,
            source_paths: source_paths,
            deleted_source_paths: deleted_source_paths,
            metadata_rows_removed: nil,
            errors: errors
          )
        end
      end
    end
  end
end
