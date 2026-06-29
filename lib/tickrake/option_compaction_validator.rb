# frozen_string_literal: true

module Tickrake
  class OptionCompactionValidator
    Result = Struct.new(
      :safe_to_delete,
      :provider_name,
      :option_root,
      :sample_date,
      :compacted_path,
      :source_paths,
      :expected_row_count,
      :actual_row_count,
      :dry_run,
      :deleted_paths,
      :metadata_rows_removed,
      :deletion_errors,
      :errors,
      keyword_init: true
    ) do
      def deletion_errors
        self[:deletion_errors] || []
      end

      def deleted_paths
        self[:deleted_paths] || []
      end
    end

    def initialize(config:, option_root:, sample_date:, provider_name:, progress_reporter: nil)
      @config = config
      @option_root = option_root.to_s
      @sample_date = sample_date
      @provider_name = provider_name.to_s
      @progress_reporter = progress_reporter
      @storage_paths = Tickrake::Storage::Paths.new(config)
    end

    def validate
      compacted_csv_path = @storage_paths.option_compacted_sample_path(
        provider: @provider_name,
        root: @option_root,
        sample_date: @sample_date,
        format: "csv"
      )
      return missing_compacted_result(compacted_csv_path) unless File.exist?(compacted_csv_path)

      dataset = Tickrake::Storage::OptionCompactionDataset.new(
        config: @config,
        provider_name: @provider_name,
        option_root: @option_root
      )
      raw_files = dataset.raw_snapshot_files(sample_date: @sample_date)
      built = dataset.build_rows(
        sample_date: @sample_date,
        raw_files: raw_files,
        progress_reporter: @progress_reporter,
        progress_title_prefix: "Validate"
      )
      compacted_headers, compacted_rows = read_compacted_csv(compacted_csv_path)
      @progress_reporter&.advance(title: "Validate #{File.basename(compacted_csv_path)}")

      errors = []
      errors << "No matching source snapshot files found." if built.fetch(:raw_files).empty?
      errors << "Compacted CSV headers do not match expected compaction headers." unless compacted_headers == built.fetch(:headers)
      if compacted_rows.length != built.fetch(:rows).length
        errors << "Compacted CSV row count #{compacted_rows.length} does not match expected row count #{built.fetch(:rows).length}."
      end

      first_mismatch = first_row_mismatch(compacted_rows, built.fetch(:rows))
      errors << first_mismatch if first_mismatch

      Result.new(
        safe_to_delete: errors.empty?,
        provider_name: @provider_name,
        option_root: @option_root,
        sample_date: @sample_date,
        compacted_path: compacted_csv_path,
        source_paths: built.fetch(:raw_files),
        expected_row_count: built.fetch(:rows).length,
        actual_row_count: compacted_rows.length,
        dry_run: nil,
        deleted_paths: [],
        metadata_rows_removed: nil,
        deletion_errors: [],
        errors: errors
      )
    ensure
      @progress_reporter&.finish
    end

    private

    def missing_compacted_result(compacted_csv_path)
      Result.new(
        safe_to_delete: false,
        provider_name: @provider_name,
        option_root: @option_root,
        sample_date: @sample_date,
        compacted_path: compacted_csv_path,
        source_paths: [],
        expected_row_count: 0,
        actual_row_count: 0,
        dry_run: nil,
        deleted_paths: [],
        metadata_rows_removed: nil,
        deletion_errors: [],
        errors: ["Compacted CSV file not found: #{compacted_csv_path}"]
      )
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
  end
end
