# frozen_string_literal: true

module Tickrake
  module Maintenance
    module OptionSamples
      class Compactor
        def initialize(context:, writer: Tickrake::Storage::DuckdbOptionCompactedWriter.new)
          @context = context
          @writer = writer
        end

        DEFAULT_ARTIFACTS = %w[parquet].freeze

        def run(artifacts: DEFAULT_ARTIFACTS, progress_reporter: nil)
          selected_artifacts = Array(artifacts).map(&:to_s)
          selected_artifacts = DEFAULT_ARTIFACTS if selected_artifacts.empty?

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

          csv_path = selected_artifacts.include?("csv") ? @context.compacted_path("csv") : nil
          parquet_path = selected_artifacts.include?("parquet") ? @context.compacted_path("parquet") : nil
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

          artifacts_written = [result.csv_path, result.parquet_path].compact

          CompactResult.new(
            success: true,
            provider_name: @context.provider_name,
            option_root: @context.option_root,
            sample_date: @context.sample_date,
            artifacts_written: artifacts_written,
            row_count: result.row_count,
            source_file_count: raw_files.length,
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

      end
    end
  end
end
