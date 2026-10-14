# frozen_string_literal: true

require "tzinfo"

module Tickrake
  module Storage
    class OptionCompactionDataset
      def initialize(config:, provider_name:, option_root:, storage_paths: Tickrake::Storage::Paths.new(config))
        @config = config
        @provider_name = provider_name
        @option_root = option_root
        @storage_paths = storage_paths
        @storage_root = Tickrake::PathSupport.sanitize_symbol(option_root)
      end

      def headers
        Tickrake::Storage::OptionSampleWriter::CSV_HEADERS + ["sampled_at"]
      end

      def raw_snapshot_files(sample_date:)
        pattern = File.join(
          @storage_paths.option_samples_dir(provider: @provider_name, sample_date: sample_date),
          "#{@storage_root}_exp*.csv"
        )
        Dir.glob(pattern).select { |path| raw_snapshot_filename?(path) }.sort_by { |path| [sampled_at_for_path(path), path] }
      end

      def build_rows(sample_date:, raw_files: nil, progress_reporter: nil, progress_title_prefix: nil)
        file_headers = nil
        rows = []
        sampled_times = []
        files = raw_files || raw_snapshot_files(sample_date: sample_date)
        target_headers = Tickrake::Storage::OptionSampleWriter::CSV_HEADERS

        files.each do |file|
          sampled_at = sampled_at_for_path(file)
          sampled_times << sampled_at
          CSV.foreach(file, headers: true) do |row|
            file_headers ||= row.headers
            unless row.headers == file_headers
              raise Tickrake::Error, "Cannot compact option snapshots with mismatched headers for #{@option_root} on #{File.basename(file)}."
            end

            field_values = target_headers.map do |header|
              val = row[header]
              if header == "quote_time_in_long" && (val.nil? || val.strip.empty?)
                (sampled_at.utc.to_f * 1000).to_i.to_s
              else
                val
              end
            end
            rows << field_values + [sampled_at.utc.iso8601]
          end
          progress_reporter&.advance(title: [progress_title_prefix, File.basename(file)].compact.join(" "))
        end

        {
          raw_files: files,
          headers: target_headers + ["sampled_at"],
          rows: rows,
          sampled_times: sampled_times
        }
      end

      def sampled_at_for_path(path)
        basename = File.basename(path, ".csv")
        match = /\A#{Regexp.escape(@storage_root)}_exp\d{4}-\d{2}-\d{2}_(?<date>\d{4}-\d{2}-\d{2})_(?<time>\d{2}-\d{2}-\d{2})\z/.match(basename)
        raise Tickrake::Error, "Unable to derive sampled_at from option snapshot path #{path}." unless match

        date = Date.iso8601(match[:date])
        hours, minutes, seconds = match[:time].split("-").map(&:to_i)
        timezone_name = @config.option_snapshot_filename_timezone.to_s
        if timezone_name.empty? || timezone_name.casecmp("utc").zero?
          Time.utc(date.year, date.month, date.day, hours, minutes, seconds)
        else
          TZInfo::Timezone.get(timezone_name).local_time(date.year, date.month, date.day, hours, minutes, seconds).to_time.utc
        end
      end

      private

      def raw_snapshot_filename?(path)
        /\A#{Regexp.escape(@storage_root)}_exp\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.csv\z/.match?(File.basename(path))
      end
    end
  end
end
