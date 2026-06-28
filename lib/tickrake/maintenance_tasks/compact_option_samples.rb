# frozen_string_literal: true

require "tzinfo"

module Tickrake
  module MaintenanceTasks
    class CompactOptionSamples
      Result = Struct.new(:task, :processed_dates, :artifacts_written, keyword_init: true)

      def initialize(runtime:, scheduled_job:, start_date: nil, end_date: nil, progress_reporter: nil, storage_paths: Tickrake::Storage::Paths.new(runtime.config), writer: Tickrake::Storage::OptionCompactedWriter.new)
        @runtime = runtime
        @scheduled_job = scheduled_job
        @start_date = start_date
        @end_date = end_date
        @progress_reporter = progress_reporter
        @storage_paths = storage_paths
        @writer = writer
      end

      def run(now: Time.now)
        processed_dates = []
        artifacts_written = []

        selected_dates(now).each do |sample_date|
          result = compact_date(sample_date: sample_date)
          @progress_reporter&.advance(title: "Compact #{sample_date.iso8601}")
          next unless result

          processed_dates << sample_date
          artifacts_written.concat(result)
        end

        Result.new(
          task: "compact_option_samples",
          processed_dates: processed_dates,
          artifacts_written: artifacts_written
        )
      ensure
        @progress_reporter&.finish
      end

      private

      def compact_date(sample_date:)
        built = compaction_dataset.build_rows(sample_date: sample_date)
        files = built.fetch(:raw_files)
        if files.empty?
          @runtime.logger.info("No raw option snapshots found for provider=#{provider_name} root=#{option_root} sample_date=#{sample_date}.")
          return nil
        end

        run_id = @runtime.tracker.record_start(
          job_type: @scheduled_job.name,
          dataset_type: "options_compacted",
          symbol: option_root,
          option_root: option_root,
          requested_buckets: nil,
          resolved_expiration: nil,
          scheduled_for: Time.now,
          started_at: Time.now
        )

        headers = built.fetch(:headers)
        rows = built.fetch(:rows)
        sampled_times = built.fetch(:sampled_times)
        csv_path = @storage_paths.option_compacted_sample_path(provider: provider_name, root: option_root, sample_date: sample_date, format: "csv")
        parquet_path = @storage_paths.option_compacted_sample_path(provider: provider_name, root: option_root, sample_date: sample_date, format: "parquet")
        @writer.write(csv_path: csv_path, parquet_path: parquet_path, headers: headers, rows: rows)

        upsert_file_metadata(
          path: csv_path,
          dataset_type: "options_compacted_csv",
          storage_format: "csv",
          sampled_times: sampled_times,
          row_count: rows.length,
          source_file_count: files.length
        )
        upsert_file_metadata(
          path: parquet_path,
          dataset_type: "options_compacted_parquet",
          storage_format: "parquet",
          sampled_times: sampled_times,
          row_count: rows.length,
          source_file_count: files.length
        )
        @runtime.tracker.record_finish(id: run_id, status: "success", finished_at: Time.now, output_path: csv_path)
        [csv_path, parquet_path]
      rescue StandardError => e
        @runtime.tracker.record_finish(id: run_id, status: "failed", finished_at: Time.now, error_message: e.message) if run_id
        raise
      end

      def selected_dates(now)
        return [now.to_date] if @start_date.nil? && @end_date.nil?

        raise Tickrake::Error, "Maintenance runs require both --start-date and --end-date." if @start_date.nil? || @end_date.nil?
        raise Tickrake::Error, "--end-date must be on or after --start-date." if @end_date < @start_date

        (@start_date..@end_date).to_a
      end

      def upsert_file_metadata(path:, dataset_type:, storage_format:, sampled_times:, row_count:, source_file_count:)
        stat = File.stat(path)
        @runtime.tracker.upsert_file_metadata(
          path: path,
          dataset_type: dataset_type,
          provider_name: provider_name,
          ticker: option_root,
          frequency: nil,
          expiration_date: nil,
          storage_format: storage_format,
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

      def option_root
        @scheduled_job.settings.fetch("option_root")
      end

      def provider_name
        @provider_name ||= @runtime.provider_override_name || @scheduled_job.provider || @scheduled_job.settings["provider"] || @runtime.config.default_provider_name
      end

      def storage_root
        @storage_root ||= Tickrake::PathSupport.sanitize_symbol(option_root)
      end

      def compaction_dataset
        @compaction_dataset ||= Tickrake::Storage::OptionCompactionDataset.new(
          config: @runtime.config,
          provider_name: provider_name,
          option_root: option_root,
          storage_paths: @storage_paths
        )
      end
    end
  end
end
