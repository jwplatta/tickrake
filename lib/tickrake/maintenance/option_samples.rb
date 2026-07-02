# frozen_string_literal: true

module Tickrake
  module Maintenance
    module OptionSamples
      ValidationResult = Struct.new(
        :safe_to_delete,
        :provider_name,
        :option_root,
        :sample_date,
        :compacted_path,
        :source_paths,
        :expected_row_count,
        :actual_row_count,
        :errors,
        keyword_init: true
      )

      CompactResult = Struct.new(
        :success,
        :provider_name,
        :option_root,
        :sample_date,
        :artifacts_written,
        :errors,
        keyword_init: true
      ) do
        def successful?
          success
        end
      end

      SourceCleanupResult = Struct.new(
        :success,
        :provider_name,
        :option_root,
        :sample_date,
        :source_paths,
        :deleted_source_paths,
        :metadata_rows_removed,
        :errors,
        keyword_init: true
      ) do
        def successful?
          success
        end
      end

      ArchiveResult = Struct.new(
        :success,
        :provider_name,
        :option_root,
        :sample_date,
        :artifact_results,
        :errors,
        keyword_init: true
      ) do
        def successful?
          success
        end

        def archived_paths
          artifact_results.map { |result| result.fetch(:path) }
        end

        def remote_uris
          artifact_results.each_with_object({}) do |result, memo|
            memo[result.fetch(:path)] = result.fetch(:remote_uri)
          end
        end
      end

      RetentionResult = Struct.new(
        :success,
        :provider_name,
        :option_root,
        :sample_date,
        :retained_local,
        :errors,
        keyword_init: true
      ) do
        def successful?
          success
        end
      end

      class Context
        attr_reader :config, :tracker, :provider_name, :option_root, :sample_date, :logger, :storage_paths

        def initialize(config:, tracker:, provider_name:, option_root:, sample_date:, logger:, storage_paths: Tickrake::Storage::Paths.new(config))
          @config = config
          @tracker = tracker
          @provider_name = provider_name.to_s
          @option_root = option_root.to_s
          @sample_date = sample_date
          @logger = logger
          @storage_paths = storage_paths
        end

        def dataset
          @dataset ||= Tickrake::Storage::OptionCompactionDataset.new(
            config: config,
            provider_name: provider_name,
            option_root: option_root,
            storage_paths: storage_paths
          )
        end

        def compacted_path(format)
          storage_paths.option_compacted_sample_path(
            provider: provider_name,
            root: option_root,
            sample_date: sample_date,
            format: format
          )
        end

        def log(level, message)
          logger&.public_send(level, "maintenance option_samples provider=#{provider_name} root=#{option_root} sample_date=#{sample_date}: #{message}")
        end
      end

      class Compactor
        def initialize(context:, writer: Tickrake::Storage::OptionCompactedWriter.new)
          @context = context
          @writer = writer
        end

        def run(progress_reporter: nil)
          raw_files = @context.dataset.raw_snapshot_files(sample_date: @context.sample_date)
          if raw_files.empty?
            @context.log(:info, "compact skipped: no raw option snapshots found")
            return CompactResult.new(
              success: true,
              provider_name: @context.provider_name,
              option_root: @context.option_root,
              sample_date: @context.sample_date,
              artifacts_written: [],
              errors: []
            )
          end

          progress_reporter&.add_total(raw_files.length - 1)
          built = @context.dataset.build_rows(
            sample_date: @context.sample_date,
            raw_files: raw_files,
            progress_reporter: progress_reporter,
            progress_title_prefix: "Compact #{@context.sample_date.iso8601}"
          )

          csv_path = @context.compacted_path("csv")
          parquet_path = @context.compacted_path("parquet")
          @writer.write(csv_path: csv_path, parquet_path: parquet_path, headers: built.fetch(:headers), rows: built.fetch(:rows))
          upsert_metadata(path: csv_path, format: "csv", sampled_times: built.fetch(:sampled_times), row_count: built.fetch(:rows).length, source_file_count: built.fetch(:raw_files).length)
          upsert_metadata(path: parquet_path, format: "parquet", sampled_times: built.fetch(:sampled_times), row_count: built.fetch(:rows).length, source_file_count: built.fetch(:raw_files).length)

          CompactResult.new(
            success: true,
            provider_name: @context.provider_name,
            option_root: @context.option_root,
            sample_date: @context.sample_date,
            artifacts_written: [csv_path, parquet_path],
            errors: []
          )
        rescue StandardError => e
          @context.log(:error, "compact failed: #{e.class}: #{e.message}")
          CompactResult.new(
            success: false,
            provider_name: @context.provider_name,
            option_root: @context.option_root,
            sample_date: @context.sample_date,
            artifacts_written: [],
            errors: [e.message]
          )
        ensure
          progress_reporter&.finish
        end

        private

        def upsert_metadata(path:, format:, sampled_times:, row_count:, source_file_count:)
          stat = File.stat(path)
          @context.tracker.upsert_file_metadata(
            path: path,
            dataset_type: format == "csv" ? "options_compacted_csv" : "options_compacted_parquet",
            provider_name: @context.provider_name,
            ticker: @context.option_root,
            frequency: nil,
            expiration_date: nil,
            storage_format: format,
            storage_location: "local",
            artifact_status: "ready_local",
            remote_uri: nil,
            source_file_count: source_file_count,
            row_count: row_count,
            first_observed_at: sampled_times.min&.utc&.iso8601,
            last_observed_at: sampled_times.max&.utc&.iso8601,
            file_mtime: stat.mtime.to_i,
            file_size: stat.size,
            updated_at: Time.now
          )
        end
      end

      class Validator
        def initialize(context:)
          @context = context
        end

        def run(progress_reporter: nil)
          compacted_path = @context.compacted_path("csv")
          compacted_headers, compacted_rows = read_compacted_csv(compacted_path)
          built = @context.dataset.build_rows(
            sample_date: @context.sample_date,
            progress_reporter: progress_reporter,
            progress_title_prefix: "Validate #{@context.sample_date.iso8601}"
          )
          progress_reporter&.advance(title: "Validate #{File.basename(compacted_path)}")

          errors = []
          errors << "No matching source snapshot files found." if built.fetch(:raw_files).empty?
          errors << "Compacted CSV headers do not match expected compaction headers." unless compacted_headers == built.fetch(:headers)
          if compacted_rows.length != built.fetch(:rows).length
            errors << "Compacted CSV row count #{compacted_rows.length} does not match expected row count #{built.fetch(:rows).length}."
          end
          mismatch = first_row_mismatch(compacted_rows, built.fetch(:rows))
          errors << mismatch if mismatch

          ValidationResult.new(
            safe_to_delete: errors.empty?,
            provider_name: @context.provider_name,
            option_root: @context.option_root,
            sample_date: @context.sample_date,
            compacted_path: compacted_path,
            source_paths: built.fetch(:raw_files),
            expected_row_count: built.fetch(:rows).length,
            actual_row_count: compacted_rows.length,
            errors: errors
          )
        rescue Errno::ENOENT
          ValidationResult.new(
            safe_to_delete: false,
            provider_name: @context.provider_name,
            option_root: @context.option_root,
            sample_date: @context.sample_date,
            compacted_path: compacted_path,
            source_paths: [],
            expected_row_count: 0,
            actual_row_count: 0,
            errors: ["Compacted CSV file not found: #{compacted_path}"]
          )
        ensure
          progress_reporter&.finish
        end

        private

        def read_compacted_csv(path)
          rows = []
          headers = nil
          CSV.foreach(path, headers: true) do |row|
            headers ||= row.headers
            rows << row.fields
          end
          [headers || [], rows]
        end

        def first_row_mismatch(actual_rows, expected_rows)
          actual_rows.zip(expected_rows).each_with_index do |(actual, expected), index|
            next if actual == expected

            return "First row mismatch at row #{index + 1}."
          end
          nil
        end
      end

      class SourceSampleCleaner
        def initialize(context:)
          @context = context
        end

        def run(source_paths:, dry_run: false)
          return build_result(source_paths: source_paths, deleted_source_paths: [], metadata_rows_removed: nil, errors: []) if dry_run

          deleted_source_paths = []
          errors = []
          source_paths.each do |path|
            File.delete(path)
            deleted_source_paths << path
          rescue StandardError => e
            errors << "Failed to delete source snapshot CSV #{path}: #{e.message}"
            break
          end

          metadata_rows_removed = deleted_source_paths.empty? ? 0 : @context.tracker.delete_file_metadata_paths(deleted_source_paths)
          build_result(source_paths: source_paths, deleted_source_paths: deleted_source_paths, metadata_rows_removed: metadata_rows_removed, errors: errors)
        rescue StandardError => e
          build_result(source_paths: source_paths, deleted_source_paths: [], metadata_rows_removed: nil, errors: [e.message])
        end

        private

        def build_result(source_paths:, deleted_source_paths:, metadata_rows_removed:, errors:)
          SourceCleanupResult.new(
            success: errors.empty?,
            provider_name: @context.provider_name,
            option_root: @context.option_root,
            sample_date: @context.sample_date,
            source_paths: source_paths,
            deleted_source_paths: deleted_source_paths,
            metadata_rows_removed: metadata_rows_removed,
            errors: errors
          )
        end
      end

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
