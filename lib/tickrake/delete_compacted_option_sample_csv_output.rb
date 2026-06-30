# frozen_string_literal: true

module Tickrake
  class DeleteCompactedOptionSampleCsvOutput
    def initialize(stdout: $stdout)
      @stdout = stdout
    end

    def emit(result)
      @stdout.puts("Compacted CSV: #{result.csv_path}")
      @stdout.puts("Provider: #{result.provider_name}")
      @stdout.puts("Option root: #{result.option_root}")
      @stdout.puts("Sample date: #{result.sample_date.iso8601}")
      @stdout.puts("Remote URI: #{result.remote_uri}")

      if result.dry_run
        @stdout.puts("Dry run: would delete the local compacted CSV and mark its metadata as remote.")
      else
        @stdout.puts("Deleted local compacted CSV and marked its metadata as remote.")
      end
    end
  end
end
