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
      @option_root = option_root
      @sample_date = sample_date
      @provider_name = provider_name
      @progress_reporter = progress_reporter
    end

    def validate
      result = Tickrake::Maintenance::OptionSamples::Processor.new(
        config: @config,
        tracker: nil,
        provider_name: @provider_name,
        option_root: @option_root,
        sample_date: @sample_date,
        logger: nil
      ).validate(progress_reporter: @progress_reporter)

      Result.new(
        safe_to_delete: result.safe_to_delete,
        provider_name: result.provider_name,
        option_root: result.option_root,
        sample_date: result.sample_date,
        compacted_path: result.compacted_path,
        source_paths: result.source_paths,
        expected_row_count: result.expected_row_count,
        actual_row_count: result.actual_row_count,
        dry_run: nil,
        deleted_paths: [],
        metadata_rows_removed: nil,
        deletion_errors: [],
        errors: result.errors
      )
    end
  end
end
