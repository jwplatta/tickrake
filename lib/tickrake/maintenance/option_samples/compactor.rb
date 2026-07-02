# frozen_string_literal: true

module Tickrake
  module Maintenance
    module OptionSamples
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
    end
  end
end
