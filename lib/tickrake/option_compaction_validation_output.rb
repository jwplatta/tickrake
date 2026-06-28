# frozen_string_literal: true

module Tickrake
  class OptionCompactionValidationOutput
    def initialize(stdout: $stdout, stderr: $stderr)
      @stdout = stdout
      @stderr = stderr
    end

    def emit(result)
      @stdout.puts("Compacted CSV: #{result.compacted_path}")
      @stdout.puts("Provider: #{result.provider_name}")
      @stdout.puts("Option root: #{result.option_root}")
      @stdout.puts("Sample date: #{result.sample_date.iso8601}")
      @stdout.puts("Source snapshots: #{result.source_paths.length}")
      @stdout.puts("Expected rows: #{result.expected_row_count}")
      @stdout.puts("Actual rows: #{result.actual_row_count}")

      if result.safe_to_delete
        @stdout.puts("Validation passed: safe to delete source snapshot CSV files.")
      else
        @stderr.puts("Validation failed: not safe to delete source snapshot CSV files.")
        result.errors.each { |error| @stderr.puts("  ERROR: #{error}") }
      end
    end
  end
end
