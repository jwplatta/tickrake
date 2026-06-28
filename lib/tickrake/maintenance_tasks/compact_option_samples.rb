# frozen_string_literal: true

require "tzinfo"

module Tickrake
  module MaintenanceTasks
    class CompactOptionSamples
      Result = Struct.new(:task, :processed_dates, :artifacts_written, keyword_init: true)

      def initialize(runtime:, scheduled_job:, start_date: nil, end_date: nil, storage_paths: Tickrake::Storage::Paths.new(runtime.config), writer: Tickrake::Storage::OptionCompactedWriter.new)
        @runtime = runtime
        @scheduled_job = scheduled_job
        @start_date = start_date
        @end_date = end_date
        @storage_paths = storage_paths
        @writer = writer
      end

      def run(now: Time.now)
        processed_dates = []
        artifacts_written = []

        selected_dates(now).each do |sample_date|
          result = compact_date(sample_date: sample_date)
          next unless result

          processed_dates << sample_date
          artifacts_written.concat(result)
        end

        Result.new(
          task: "compact_option_samples",
          processed_dates: processed_dates,
          artifacts_written: artifacts_written
        )
      end

      private

      def compact_date(sample_date:)
        files = raw_snapshot_files(sample_date: sample_date)
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

        headers, rows, sampled_times = read_rows(files)
        csv_path = @storage_paths.option_compacted_sample_path(provider: provider_name, root: option_root, sample_date: sample_date, format: "csv")
        parquet_path = @storage_paths.option_compacted_sample_path(provider: provider_name, root: option_root, sample_date: sample_date, format: "parquet")
        @writer.write(csv_path: csv_path, parquet_path: parquet_path, headers: headers + ["sampled_at"], rows: rows)

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

      def read_rows(files)
        headers = nil
        rows = []
        sampled_times = []

        files.each do |file|
          sampled_at = sampled_at_for_path(file)
          sampled_times << sampled_at
          CSV.foreach(file, headers: true) do |row|
            headers ||= row.headers
            unless row.headers == headers
              raise Tickrake::Error, "Cannot compact option snapshots with mismatched headers for #{option_root} on #{File.basename(file)}."
            end

            rows << row.fields + [sampled_at.utc.iso8601]
          end
        end

        [headers || Tickrake::Storage::OptionSampleWriter::CSV_HEADERS, rows, sampled_times]
      end

      def raw_snapshot_files(sample_date:)
        pattern = File.join(
          @storage_paths.option_samples_dir(provider: provider_name, sample_date: sample_date),
          "#{storage_root}_exp*.csv"
        )
        Dir.glob(pattern).select { |path| raw_snapshot_filename?(path) }.sort_by { |path| [sampled_at_for_path(path), path] }
      end

      def raw_snapshot_filename?(path)
        /\A#{Regexp.escape(storage_root)}_exp\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.csv\z/.match?(File.basename(path))
      end

      def sampled_at_for_path(path)
        basename = File.basename(path, ".csv")
        match = /\A#{Regexp.escape(storage_root)}_exp\d{4}-\d{2}-\d{2}_(?<date>\d{4}-\d{2}-\d{2})_(?<time>\d{2}-\d{2}-\d{2})\z/.match(basename)
        raise Tickrake::Error, "Unable to derive sampled_at from option snapshot path #{path}." unless match

        date = Date.iso8601(match[:date])
        hours, minutes, seconds = match[:time].split("-").map(&:to_i)
        timezone_name = @runtime.config.option_snapshot_filename_timezone.to_s
        if timezone_name.empty? || timezone_name.casecmp("utc").zero?
          Time.utc(date.year, date.month, date.day, hours, minutes, seconds)
        else
          TZInfo::Timezone.get(timezone_name).local_time(date.year, date.month, date.day, hours, minutes, seconds).to_time.utc
        end
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
    end
  end
end
