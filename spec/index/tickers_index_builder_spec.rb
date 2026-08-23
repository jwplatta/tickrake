# frozen_string_literal: true

require_relative "../spec_helper"
require "tmpdir"

RSpec.describe Tickrake::Index::TickersIndexBuilder do
  def make_tracker(dir)
    Tickrake::Tracker.new(File.join(dir, "tickrake.sqlite3"))
  end

  describe "#build" do
    it "produces the expected top-level shape" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        result = described_class.new(tracker: tracker).build(provider: "schwab")

        expect(result["schema_version"]).to eq(1)
        expect(result["provider"]).to eq("schwab")
        expect(result).to have_key("updated_at")
        expect(result["roots"]).to eq([])
      end
    end

    it "lists all known roots for the provider" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        stat = Time.now.to_i
        [["SPXW", "options_compacted_parquet"], ["SPY", "options_compacted_parquet"], ["QQQ", "options"]].each do |root, dtype|
          tracker.upsert_file_metadata(
            path: "#{dir}/#{root}_sample.csv",
            dataset_type: dtype,
            provider_name: "schwab",
            ticker: root,
            row_count: 100,
            first_observed_at: "2026-08-21T13:00:00Z",
            last_observed_at: "2026-08-21T20:00:00Z",
            file_mtime: stat,
            file_size: 512
          )
        end

        result = described_class.new(tracker: tracker).build(provider: "schwab")
        expect(result["roots"]).to eq(["QQQ", "SPXW", "SPY"])
      end
    end

    it "excludes roots from other providers" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        stat = Time.now.to_i
        tracker.upsert_file_metadata(
          path: "#{dir}/SPXW_sample.parquet",
          dataset_type: "options_compacted_parquet",
          provider_name: "schwab",
          ticker: "SPXW",
          row_count: 100,
          first_observed_at: "2026-08-21T13:00:00Z",
          last_observed_at: "2026-08-21T20:00:00Z",
          file_mtime: stat,
          file_size: 512
        )
        tracker.upsert_file_metadata(
          path: "#{dir}/SPXW_ibkr_sample.parquet",
          dataset_type: "options_compacted_parquet",
          provider_name: "ibkr",
          ticker: "SPXW",
          row_count: 100,
          first_observed_at: "2026-08-21T13:00:00Z",
          last_observed_at: "2026-08-21T20:00:00Z",
          file_mtime: stat,
          file_size: 512
        )

        result = described_class.new(tracker: tracker).build(provider: "schwab")
        expect(result["roots"]).to eq(["SPXW"])
      end
    end
  end
end
