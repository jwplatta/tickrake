# frozen_string_literal: true

module Tickrake
  class DeleteCompactedOptionSamplesOutput
    def initialize(stdout: $stdout, stderr: $stderr)
      @stdout = stdout
      @stderr = stderr
    end

    def emit(result)
      @stdout.puts("Compacted CSV: #{result.compacted_path}")
      @stdout.puts("Provider: #{result.provider_name}")
      @stdout.puts("Option root: #{result.option_root}")
      @stdout.puts("Sample date: #{result.sample_date.iso8601}")
      @stdout.puts("Source snapshots selected: #{result.source_paths.length}")
      @stdout.puts("Expected rows: #{result.expected_row_count}")
      @stdout.puts("Actual rows: #{result.actual_row_count}")

      unless result.safe_to_delete
        @stderr.puts("Deletion aborted: compacted CSV did not validate against source snapshots.")
        result.errors.each { |error| @stderr.puts("  ERROR: #{error}") }
        return
      end

      if result.dry_run
        @stdout.puts("Dry run: would delete #{result.source_paths.length} source snapshot CSV files and remove matching metadata rows.")
        return
      end

      @stdout.puts("Deleted source snapshots: #{result.deleted_paths.length}")
      @stdout.puts("Metadata rows removed: #{result.metadata_rows_removed}")

      if result.deletion_errors.empty?
        @stdout.puts("Deletion completed: removed validated source snapshot CSV files and metadata rows.")
      else
        @stderr.puts("Deletion finished with errors.")
        result.deletion_errors.each { |error| @stderr.puts("  ERROR: #{error}") }
      end
    end
  end
end
