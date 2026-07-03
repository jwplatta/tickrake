# frozen_string_literal: true

module Tickrake
  module Maintenance
    module OptionSamples
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
          expected_rows = built.fetch(:rows)
          if compacted_rows.length != expected_rows.length
            errors << "Compacted CSV row count #{compacted_rows.length} does not match expected row count #{expected_rows.length}."
          end
          mismatch = first_row_mismatch(sorted_rows(compacted_rows), sorted_rows(expected_rows))
          errors << mismatch if mismatch

          ValidationResult.new(
            safe_to_delete: errors.empty?,
            provider_name: @context.provider_name,
            option_root: @context.option_root,
            sample_date: @context.sample_date,
            compacted_path: compacted_path,
            source_paths: built.fetch(:raw_files),
            expected_row_count: expected_rows.length,
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

        def sorted_rows(rows)
          rows.sort_by do |row|
            [
              row.fetch(30),
              row.fetch(4),
              row.fetch(0),
              row.fetch(3).to_f,
              row.fetch(1)
            ]
          end
        end
      end
    end
  end
end
