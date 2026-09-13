# frozen_string_literal: true

require_relative "../spec_helper"
require "tmpdir"
require "json"

RSpec.describe Tickrake::Index::Publisher do
  def make_tracker(dir)
    Tickrake::Tracker.new(File.join(dir, "tickrake.sqlite3"))
  end

  def upsert_compacted(tracker, dir, provider:, root:, sample_date:, format:, remote_uri: nil, storage_location: "local")
    path = File.join(dir, "#{root}_samples_#{sample_date}.#{format}")
    tracker.upsert_file_metadata(
      path: path,
      dataset_type: format == "parquet" ? "options_compacted_parquet" : "options_compacted_csv",
      provider_name: provider,
      ticker: root,
      storage_format: format,
      storage_location: storage_location,
      artifact_status: storage_location == "local" ? "ready_local" : "ready_local_and_remote",
      remote_uri: remote_uri,
      source_file_count: 5,
      row_count: 100,
      first_observed_at: "#{sample_date}T13:30:00Z",
      last_observed_at: "#{sample_date}T20:00:00Z",
      file_mtime: Time.now.to_i,
      file_size: 1024
    )
  end

  describe "#publish" do
    it "does not raise when called (publisher is a no-op stub)" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        upsert_compacted(tracker, dir, provider: "schwab", root: "SPXW",
          sample_date: "2026-08-21", format: "parquet")

        expect {
          described_class.new(tracker: tracker, options_dir: dir, logger: nil)
            .publish(provider: "schwab", root: "SPXW")
        }.not_to raise_error
      end
    end
  end
end
