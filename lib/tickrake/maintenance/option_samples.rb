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
        :validation,
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
          artifact_results.filter_map { |result| result[:path] if result[:uploaded] }
        end
      end

      CleanupResult = Struct.new(
        :success,
        :provider_name,
        :option_root,
        :sample_date,
        :source_paths,
        :deleted_source_paths,
        :retained_local,
        :remote_uris,
        :errors,
        keyword_init: true
      ) do
        def successful?
          success
        end
      end

      class Processor
        DEFAULT_RETAIN_LOCAL = { "csv" => false, "parquet" => true }.freeze

        def initialize(config:, tracker:, provider_name:, option_root:, sample_date:, logger:, storage_paths: Tickrake::Storage::Paths.new(config), writer: Tickrake::Storage::OptionCompactedWriter.new, archive_services: {})
          @config = config
          @tracker = tracker
          @provider_name = provider_name.to_s
          @option_root = option_root.to_s
          @sample_date = sample_date
          @logger = logger
          @storage_paths = storage_paths
          @writer = writer
          @archive_services = archive_services
        end

        def compact(delete_sources:, progress_reporter: nil)
          raw_files = dataset.raw_snapshot_files(sample_date: @sample_date)
          if raw_files.empty?
            log(:info, "compact skipped: no raw option snapshots found")
            return CompactResult.new(
              success: true,
              provider_name: @provider_name,
              option_root: @option_root,
              sample_date: @sample_date,
              artifacts_written: [],
              validation: ValidationResult.new(
                safe_to_delete: false,
                provider_name: @provider_name,
                option_root: @option_root,
                sample_date: @sample_date,
                compacted_path: compacted_csv_path,
                source_paths: [],
                expected_row_count: 0,
                actual_row_count: 0,
                errors: ["No raw option snapshots found."]
              ),
              deleted_source_paths: [],
              metadata_rows_removed: nil,
              errors: []
            )
          end

          progress_reporter&.add_total(raw_files.length - 1)
          built = dataset.build_rows(
            sample_date: @sample_date,
            raw_files: raw_files,
            progress_reporter: progress_reporter,
            progress_title_prefix: "Compact #{@sample_date.iso8601}"
          )
          @writer.write(csv_path: compacted_csv_path, parquet_path: compacted_parquet_path, headers: built.fetch(:headers), rows: built.fetch(:rows))
          upsert_compacted_metadata(path: compacted_csv_path, format: "csv", sampled_times: built.fetch(:sampled_times), row_count: built.fetch(:rows).length, source_file_count: built.fetch(:raw_files).length)
          upsert_compacted_metadata(path: compacted_parquet_path, format: "parquet", sampled_times: built.fetch(:sampled_times), row_count: built.fetch(:rows).length, source_file_count: built.fetch(:raw_files).length)

          validation = validate(progress_reporter: nil)
          unless validation.safe_to_delete
            log(:error, "compact validation failed: #{validation.errors.join('; ')}")
            return CompactResult.new(
              success: false,
              provider_name: @provider_name,
              option_root: @option_root,
              sample_date: @sample_date,
              artifacts_written: [compacted_csv_path, compacted_parquet_path],
              validation: validation,
              deleted_source_paths: [],
              metadata_rows_removed: nil,
              errors: validation.errors
            )
          end

          deleted_source_paths = []
          metadata_rows_removed = nil
          if delete_sources
            deleted_source_paths, metadata_rows_removed = delete_source_paths(validation.source_paths)
            log(:info, "deleted #{deleted_source_paths.length} raw source snapshot CSVs after successful compaction validation")
          else
            log(:info, "compaction validation succeeded; raw source snapshots retained")
          end

          CompactResult.new(
            success: true,
            provider_name: @provider_name,
            option_root: @option_root,
            sample_date: @sample_date,
            artifacts_written: [compacted_csv_path, compacted_parquet_path],
            validation: validation,
            deleted_source_paths: deleted_source_paths,
            metadata_rows_removed: metadata_rows_removed,
            errors: []
          )
        rescue StandardError => e
          log(:error, "compact failed: #{e.class}: #{e.message}")
          CompactResult.new(
            success: false,
            provider_name: @provider_name,
            option_root: @option_root,
            sample_date: @sample_date,
            artifacts_written: [],
            validation: nil,
            deleted_source_paths: [],
            metadata_rows_removed: nil,
            errors: [e.message]
          )
        end

        def validate(progress_reporter: nil)
          compacted_headers, compacted_rows = read_compacted_csv(compacted_csv_path)
          built = dataset.build_rows(
            sample_date: @sample_date,
            progress_reporter: progress_reporter,
            progress_title_prefix: "Validate #{@sample_date.iso8601}"
          )
          progress_reporter&.advance(title: "Validate #{File.basename(compacted_csv_path)}")

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
            provider_name: @provider_name,
            option_root: @option_root,
            sample_date: @sample_date,
            compacted_path: compacted_csv_path,
            source_paths: built.fetch(:raw_files),
            expected_row_count: built.fetch(:rows).length,
            actual_row_count: compacted_rows.length,
            errors: errors
          )
        rescue Errno::ENOENT
          ValidationResult.new(
            safe_to_delete: false,
            provider_name: @provider_name,
            option_root: @option_root,
            sample_date: @sample_date,
            compacted_path: compacted_csv_path,
            source_paths: [],
            expected_row_count: 0,
            actual_row_count: 0,
            errors: ["Compacted CSV file not found: #{compacted_csv_path}"]
          )
        ensure
          progress_reporter&.finish
        end

        def archive(destination_name:, artifacts:, retain_local:)
          archive_service = archive_service_for(destination_name)
          selected_artifacts = artifacts.empty? ? %w[csv parquet] : artifacts
          artifact_results = []
          errors = []

          selected_artifacts.each do |artifact|
            path = compacted_path_for(artifact)
            log(:info, "archive start artifact=#{artifact} destination=#{destination_name} path=#{path}")
            raise Tickrake::Error, "Compacted artifact not found: #{path}" unless File.exist?(path)

            remote_object = archive_service.upload(path)
            remote_object = archive_service.verify(path)
            local_size = File.size(path)
            if remote_object.size != local_size
              raise Tickrake::Error, "Archived object size mismatch for #{path}: local=#{local_size} remote=#{remote_object.size}"
            end

            keep_local = retain_local.fetch(artifact, DEFAULT_RETAIN_LOCAL.fetch(artifact))
            if keep_local
              update_metadata_for_local_archive(path: path, remote_uri: remote_object.uri)
            else
              delete_local_artifact(path: path, remote_uri: remote_object.uri)
            end
            log(:info, "archive finished artifact=#{artifact} destination=#{destination_name} remote_uri=#{remote_object.uri} retain_local=#{keep_local}")

            artifact_results << {
              artifact: artifact,
              path: path,
              uploaded: true,
              remote_uri: remote_object.uri,
              retained_local: keep_local
            }
          rescue StandardError => e
            log(:error, "archive failed artifact=#{artifact} destination=#{destination_name}: #{e.class}: #{e.message}")
            artifact_results << {
              artifact: artifact,
              path: path,
              uploaded: false,
              remote_uri: nil,
              retained_local: true
            }
            errors << e.message
          end

          ArchiveResult.new(
            success: errors.empty?,
            provider_name: @provider_name,
            option_root: @option_root,
            sample_date: @sample_date,
            artifact_results: artifact_results,
            errors: errors
          )
        end

        def cleanup(destination_name:, delete_sources:, retain_local:, dry_run: false)
          remote_uris = {}
          retained_local = {}
          errors = []

          %w[csv parquet].each do |artifact|
            path = compacted_path_for(artifact)
            raise Tickrake::Error, "Local compacted #{artifact.upcase} not found: #{path}" unless File.exist?(path)

            remote_object = archive_service_for(destination_name).verify(path)
            local_size = File.size(path)
            if remote_object.size != local_size
              raise Tickrake::Error, "Archived object size mismatch for #{path}: local=#{local_size} remote=#{remote_object.size}"
            end
            remote_uris[path] = remote_object.uri
          end

          validation = validate
          unless validation.safe_to_delete
            return CleanupResult.new(
              success: false,
              provider_name: @provider_name,
              option_root: @option_root,
              sample_date: @sample_date,
              source_paths: validation.source_paths,
              deleted_source_paths: [],
              retained_local: { "csv" => true, "parquet" => true },
              remote_uris: remote_uris,
              errors: validation.errors
            )
          end

          deleted_source_paths = []
          if delete_sources && !dry_run
            deleted_source_paths, = delete_source_paths(validation.source_paths)
          end

          retained_local = {}
          %w[csv parquet].each do |artifact|
            path = compacted_path_for(artifact)
            keep_local = retain_local.fetch(artifact, DEFAULT_RETAIN_LOCAL.fetch(artifact))
            if dry_run
              retained_local[artifact] = keep_local
              next
            end

            if keep_local
              update_metadata_for_local_archive(path: path, remote_uri: remote_uris.fetch(path))
            else
              delete_local_artifact(path: path, remote_uri: remote_uris.fetch(path))
            end
            retained_local[artifact] = keep_local
          end

          CleanupResult.new(
            success: true,
            provider_name: @provider_name,
            option_root: @option_root,
            sample_date: @sample_date,
            source_paths: validation.source_paths,
            deleted_source_paths: deleted_source_paths,
            retained_local: retained_local,
            remote_uris: remote_uris,
            errors: []
          )
        rescue StandardError => e
          CleanupResult.new(
            success: false,
            provider_name: @provider_name,
            option_root: @option_root,
            sample_date: @sample_date,
            source_paths: [],
            deleted_source_paths: [],
            retained_local: { "csv" => true, "parquet" => true },
            remote_uris: remote_uris || {},
            errors: [e.message]
          )
        end

        private

        def dataset
          @dataset ||= Tickrake::Storage::OptionCompactionDataset.new(
            config: @config,
            provider_name: @provider_name,
            option_root: @option_root,
            storage_paths: @storage_paths
          )
        end

        def compacted_csv_path
          @compacted_csv_path ||= @storage_paths.option_compacted_sample_path(
            provider: @provider_name,
            root: @option_root,
            sample_date: @sample_date,
            format: "csv"
          )
        end

        def compacted_parquet_path
          @compacted_parquet_path ||= @storage_paths.option_compacted_sample_path(
            provider: @provider_name,
            root: @option_root,
            sample_date: @sample_date,
            format: "parquet"
          )
        end

        def compacted_path_for(artifact)
          artifact == "csv" ? compacted_csv_path : compacted_parquet_path
        end

        def archive_service_for(destination_name)
          return @archive_services.fetch(destination_name) if @archive_services.key?(destination_name)

          archive_config = @config.archives.fetch(destination_name)
          Tickrake::Storage::S3Archive.new(@config, archive_config: archive_config)
        end

        def upsert_compacted_metadata(path:, format:, sampled_times:, row_count:, source_file_count:)
          stat = File.stat(path)
          @tracker.upsert_file_metadata(
            path: path,
            dataset_type: format == "csv" ? "options_compacted_csv" : "options_compacted_parquet",
            provider_name: @provider_name,
            ticker: @option_root,
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

        def delete_source_paths(paths)
          deleted_paths = []
          paths.each do |path|
            File.delete(path)
            deleted_paths << path
          end
          [deleted_paths, @tracker.delete_file_metadata_paths(deleted_paths)]
        end

        def update_metadata_for_local_archive(path:, remote_uri:)
          metadata = fetch_metadata(path)
          @tracker.upsert_file_metadata(
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

        def delete_local_artifact(path:, remote_uri:)
          metadata = fetch_metadata(path)
          original_mtime = metadata["file_mtime"]
          File.delete(path)
          @tracker.upsert_file_metadata(
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
          metadata = @tracker.file_metadata(path)
          raise Tickrake::Error, "Compacted artifact metadata not found: #{path}" unless metadata

          metadata
        end

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

        def log(level, message)
          @logger&.public_send(level, "maintenance option_samples provider=#{@provider_name} root=#{@option_root} sample_date=#{@sample_date}: #{message}")
        end
      end
    end
  end
end
