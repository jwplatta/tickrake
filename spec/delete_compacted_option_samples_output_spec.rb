# frozen_string_literal: true

RSpec.describe Tickrake::DeleteCompactedOptionSamplesOutput do
  it "emits a dry-run summary without listing every source path" do
    stdout = StringIO.new
    stderr = StringIO.new
    result = Tickrake::OptionCompactionValidator::Result.new(
      safe_to_delete: true,
      provider_name: "schwab",
      option_root: "SPXW",
      sample_date: Date.new(2026, 6, 26),
      compacted_path: "/tmp/SPXW_samples_2026-06-26.csv",
      source_paths: ["/tmp/a.csv", "/tmp/b.csv"],
      expected_row_count: 2,
      actual_row_count: 2,
      dry_run: true,
      deleted_paths: [],
      metadata_rows_removed: nil,
      deletion_errors: [],
      errors: []
    )

    described_class.new(stdout: stdout, stderr: stderr).emit(result)

    expect(stdout.string).to include("Dry run: would delete 2 source snapshot CSV files and remove matching metadata rows.")
    expect(stdout.string).not_to include("DELETE:")
    expect(stderr.string).to eq("")
  end
end
