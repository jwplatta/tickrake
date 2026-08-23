# frozen_string_literal: true

require_relative "../spec_helper"
require "tmpdir"

RSpec.describe Tickrake::Index::RootIndexBuilder do
  def make_tracker(dir)
    Tickrake::Tracker.new(File.join(dir, "tickrake.sqlite3"))
  end

  def upsert_compacted(tracker, provider:, root:, sample_date:, format:, path:, row_count: 100, source_file_count: 5, remote_uri: nil, storage_location: "local", artifact_status: "ready_local")
    stat_time = Time.now.to_i
    tracker.upsert_file_metadata(
      path: path,
      dataset_type: format == "parquet" ? "options_compacted_parquet" : "options_compacted_csv",
      provider_name: provider,
      ticker: root,
      storage_format: format,
      storage_location: storage_location,
      artifact_status: artifact_status,
      remote_uri: remote_uri,
      source_file_count: source_file_count,
      row_count: row_count,
      first_observed_at: "#{sample_date}T13:30:00Z",
      last_observed_at: "#{sample_date}T20:00:00Z",
      file_mtime: stat_time,
      file_size: 1024
    )
  end

  def upsert_raw(tracker, provider:, root:, path:, expiration_date:, collection_id:, sampled_at:)
    stat_time = Time.now.to_i
    tracker.upsert_file_metadata(
      path: path,
      dataset_type: "options",
      provider_name: provider,
      ticker: root,
      storage_format: "csv",
      storage_location: "local",
      expiration_date: expiration_date,
      collection_id: collection_id,
      row_count: 510,
      first_observed_at: sampled_at,
      last_observed_at: sampled_at,
      file_mtime: stat_time,
      file_size: 512
    )
  end

  describe "#build" do
    it "produces the expected top-level JSON shape" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        builder = described_class.new(tracker: tracker, options_dir: dir)
        result = builder.build(provider: "schwab", root: "SPXW")

        expect(result["schema_version"]).to eq(1)
        expect(result["provider"]).to eq("schwab")
        expect(result["root"]).to eq("SPXW")
        expect(result).to have_key("updated_at")
        expect(result["historical"]).to eq([])
        expect(result["intraday"]).to be_nil
      end
    end

    it "builds the historical array from compacted parquet and csv rows" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        parquet_path = "#{dir}/schwab/2026/08/21/SPXW_samples_2026-08-21.parquet"
        csv_path = "#{dir}/schwab/2026/08/21/SPXW_samples_2026-08-21.csv"

        upsert_compacted(tracker,
          provider: "schwab", root: "SPXW", sample_date: "2026-08-21",
          format: "parquet", path: parquet_path, row_count: 4332, source_file_count: 9)
        upsert_compacted(tracker,
          provider: "schwab", root: "SPXW", sample_date: "2026-08-21",
          format: "csv", path: csv_path, row_count: 4332, source_file_count: 9)

        builder = described_class.new(tracker: tracker, options_dir: dir)
        result = builder.build(provider: "schwab", root: "SPXW")

        expect(result["historical"].length).to eq(1)
        entry = result["historical"].first
        expect(entry["sample_date"]).to eq("2026-08-21")
        expect(entry["status"]).to eq("ready")
        expect(entry["files"]["parquet"]["uri"]).to eq("file://#{parquet_path}")
        expect(entry["files"]["parquet"]["row_count"]).to eq(4332)
        expect(entry["files"]["csv"]["uri"]).to eq("file://#{csv_path}")
        expect(entry["first_observed_at"]).to eq("2026-08-21T13:30:00Z")
        expect(entry["last_observed_at"]).to eq("2026-08-21T20:00:00Z")
      end
    end

    it "uses remote_uri for archived artifacts" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        path = "#{dir}/schwab/2026/08/21/SPXW_samples_2026-08-21.parquet"
        remote = "s3://tickrake/data/options/schwab/2026/08/21/SPXW_samples_2026-08-21.parquet"

        upsert_compacted(tracker,
          provider: "schwab", root: "SPXW", sample_date: "2026-08-21",
          format: "parquet", path: path, remote_uri: remote,
          storage_location: "ready_local_and_remote", artifact_status: "ready_local_and_remote")

        builder = described_class.new(tracker: tracker, options_dir: dir)
        result = builder.build(provider: "schwab", root: "SPXW")

        expect(result["historical"].first["files"]["parquet"]["uri"]).to eq(remote)
      end
    end

    it "sorts historical entries by sample_date ascending" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)

        ["2026-08-20", "2026-08-19", "2026-08-21"].each do |date|
          upsert_compacted(tracker,
            provider: "schwab", root: "SPXW", sample_date: date,
            format: "parquet", path: "#{dir}/SPXW_samples_#{date}.parquet")
        end

        builder = described_class.new(tracker: tracker, options_dir: dir)
        result = builder.build(provider: "schwab", root: "SPXW")

        dates = result["historical"].map { |e| e["sample_date"] }
        expect(dates).to eq(["2026-08-19", "2026-08-20", "2026-08-21"])
      end
    end

    it "builds the intraday section from the latest collection" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        collection_id = "options-20260823T154210Z"
        sampled_at = "2026-08-23T15:42:10Z"

        upsert_raw(tracker,
          provider: "schwab", root: "SPXW",
          path: "#{dir}/schwab/2026/08/23/SPXW_exp2026-08-23_2026-08-23_15-42-10.csv",
          expiration_date: "2026-08-23", collection_id: collection_id, sampled_at: sampled_at)
        upsert_raw(tracker,
          provider: "schwab", root: "SPXW",
          path: "#{dir}/schwab/2026/08/23/SPXW_exp2026-08-24_2026-08-23_15-42-10.csv",
          expiration_date: "2026-08-24", collection_id: collection_id, sampled_at: sampled_at)

        builder = described_class.new(tracker: tracker, options_dir: dir)
        result = builder.build(provider: "schwab", root: "SPXW")

        intraday = result["intraday"]
        expect(intraday).not_to be_nil
        expect(intraday["collection_id"]).to eq(collection_id)
        expect(intraday["sample_date"]).to eq("2026-08-23")
        expect(intraday["status"]).to eq("complete")
        expect(intraday["files"].length).to eq(2)
        expect(intraday["files"].first["expiration_date"]).to eq("2026-08-23")
        expect(intraday["files"].first["uri"]).to start_with("file://")
      end
    end

    it "returns intraday null when no raw options rows exist" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        builder = described_class.new(tracker: tracker, options_dir: dir)
        result = builder.build(provider: "schwab", root: "SPXW")
        expect(result["intraday"]).to be_nil
      end
    end

    it "only includes rows for the requested provider and root" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        upsert_compacted(tracker,
          provider: "schwab", root: "SPXW", sample_date: "2026-08-21",
          format: "parquet", path: "#{dir}/SPXW_samples_2026-08-21.parquet")
        upsert_compacted(tracker,
          provider: "schwab", root: "SPY", sample_date: "2026-08-21",
          format: "parquet", path: "#{dir}/SPY_samples_2026-08-21.parquet")

        builder = described_class.new(tracker: tracker, options_dir: dir)
        result = builder.build(provider: "schwab", root: "SPXW")

        expect(result["historical"].length).to eq(1)
        expect(result["historical"].first["sample_date"]).to eq("2026-08-21")
      end
    end
  end
end
