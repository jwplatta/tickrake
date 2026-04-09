# frozen_string_literal: true

RSpec.describe Tickrake::Tracker do
  it "records run lifecycle rows in sqlite" do
    Dir.mktmpdir do |dir|
      tracker = described_class.new(File.join(dir, "tickrake.sqlite3"))
      id = tracker.record_start(
        job_type: "options_monitor",
        dataset_type: "options",
        symbol: "SPY",
        option_root: nil,
        requested_buckets: [0, 1],
        resolved_expiration: "2025-07-21",
        scheduled_for: Time.utc(2025, 7, 20, 14, 30, 0),
        started_at: Time.utc(2025, 7, 20, 14, 30, 1)
      )

      tracker.record_finish(id: id, status: "success", finished_at: Time.utc(2025, 7, 20, 14, 30, 2), output_path: "/tmp/out.csv")
      row = tracker.fetch_runs.first

      expect(row["status"]).to eq("success")
      expect(row["output_path"]).to eq("/tmp/out.csv")
      expect(row["requested_buckets"]).to include("0")
    end
  end
end
