# frozen_string_literal: true

module Tickrake
  module Maintenance
    module OptionSamples
      class Compactor
        def initialize(context:, writer: Tickrake::Storage::DuckdbOptionCompactedWriter.new)
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

          csv_path = @context.compacted_path("csv")
          parquet_path = @context.compacted_path("parquet")
          progress_reporter&.add_total(raw_files.length)
          raw_files.each do |path|
            progress_reporter&.advance(title: "Compact #{@context.sample_date.iso8601} #{File.basename(path)}")
          end

          result = @writer.write(
            raw_files: raw_files,
            csv_path: csv_path,
            parquet_path: parquet_path,
            sampled_at_resolver: @context.dataset.method(:sampled_at_for_path)
          )

          upsert_metadata(path: csv_path, format: "csv", first_sampled_at: result.first_sampled_at, last_sampled_at: result.last_sampled_at, row_count: result.row_count, source_file_count: raw_files.length)
          upsert_metadata(path: parquet_path, format: "parquet", first_sampled_at: result.first_sampled_at, last_sampled_at: result.last_sampled_at, row_count: result.row_count, source_file_count: raw_files.length)

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

        def upsert_metadata(path:, format:, first_sampled_at:, last_sampled_at:, row_count:, source_file_count:)
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
            first_observed_at: first_sampled_at&.utc&.iso8601,
            last_observed_at: last_sampled_at&.utc&.iso8601,
            file_mtime: stat.mtime.to_i,
            file_size: stat.size,
            updated_at: Time.now
          )
        end
      end
    end
  end
end
