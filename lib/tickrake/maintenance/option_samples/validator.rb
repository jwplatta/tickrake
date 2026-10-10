# frozen_string_literal: true

module Tickrake
  module Maintenance
    module OptionSamples
      class Validator
        def initialize(context:)
          @context = context
        end

        def run(progress_reporter: nil)
          parquet_path = @context.compacted_path("parquet")
          csv_path = @context.compacted_path("csv")

          compacted_path, format =
            if File.exist?(parquet_path)
              [parquet_path, :parquet]
            elsif File.exist?(csv_path)
              [csv_path, :csv]
            else
              [parquet_path, :missing]
            end

          if format == :missing
            return ValidationResult.new(
              safe_to_delete: false,
              provider_name: @context.provider_name,
              option_root: @context.option_root,
              sample_date: @context.sample_date,
              compacted_path: compacted_path,
              source_paths: [],
              expected_row_count: 0,
              actual_row_count: 0,
              errors: ["Compacted artifact not found: #{parquet_path} or #{csv_path}"]
            )
          end

          compacted_headers, compacted_rows =
            if format == :parquet
              read_compacted_parquet(compacted_path)
            else
              read_compacted_csv(compacted_path)
            end

          built = @context.dataset.build_rows(
            sample_date: @context.sample_date,
            progress_reporter: progress_reporter,
            progress_title_prefix: "Validate #{@context.sample_date.iso8601}"
          )
          progress_reporter&.advance(title: "Validate #{File.basename(compacted_path)}")

          errors = []
          errors << "No matching source snapshot files found." if built.fetch(:raw_files).empty?
          errors << "Compacted #{format.upcase} headers do not match expected compaction headers." unless compacted_headers == built.fetch(:headers)
          expected_rows = built.fetch(:rows)
          if compacted_rows.length != expected_rows.length
            errors << "Compacted #{format.upcase} row count #{compacted_rows.length} does not match expected row count #{expected_rows.length}."
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
        rescue Errno::ENOENT => e
          ValidationResult.new(
            safe_to_delete: false,
            provider_name: @context.provider_name,
            option_root: @context.option_root,
            sample_date: @context.sample_date,
            compacted_path: compacted_path || @context.compacted_path("parquet"),
            source_paths: [],
            expected_row_count: 0,
            actual_row_count: 0,
            errors: ["Compacted file not found: #{e.message}"]
          )
        ensure
          progress_reporter&.finish
        end

        private

        def read_compacted_parquet(path)
          sql_path = "'#{path.to_s.gsub("'", "''")}'"
          DuckDB::Database.open do |db|
            db.connect do |con|
              res = con.query("SELECT * FROM read_parquet(#{sql_path})")
              headers = res.columns.map(&:name)
              rows = []
              res.each do |row|
                rows << row.map do |value|
                  case value
                  when Time
                    value.utc.iso8601
                  when Date
                    value.iso8601
                  when Float
                    value.to_s
                  when Integer
                    value.to_s
                  else
                    value&.to_s
                  end
                end
              end
              [headers, rows]
            end
          end
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
            next if row_matches?(actual, expected)

            return "First row mismatch at row #{index + 1}."
          end
          nil
        end

        def row_matches?(actual, expected)
          return false unless actual.length == expected.length

          actual.zip(expected).all? do |act, exp|
            next true if act == exp
            next false if act.nil? || exp.nil?

            Float(act) == Float(exp) rescue false
          end
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
