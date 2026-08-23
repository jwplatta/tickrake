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
    it "writes ROOT.json and tickers.json to the options_dir" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        upsert_compacted(tracker, dir, provider: "schwab", root: "SPXW",
          sample_date: "2026-08-21", format: "parquet")

        described_class.new(tracker: tracker, options_dir: dir, logger: nil)
          .publish(provider: "schwab", root: "SPXW")

        root_json_path = File.join(dir, "schwab", "SPXW.json")
        tickers_json_path = File.join(dir, "schwab", "tickers.json")

        expect(File.exist?(root_json_path)).to be true
        expect(File.exist?(tickers_json_path)).to be true
      end
    end

    it "writes a valid ROOT.json with the historical entry" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        upsert_compacted(tracker, dir, provider: "schwab", root: "SPXW",
          sample_date: "2026-08-21", format: "parquet")

        described_class.new(tracker: tracker, options_dir: dir, logger: nil)
          .publish(provider: "schwab", root: "SPXW")

        payload = JSON.parse(File.read(File.join(dir, "schwab", "SPXW.json")))
        expect(payload["schema_version"]).to eq(1)
        expect(payload["provider"]).to eq("schwab")
        expect(payload["root"]).to eq("SPXW")
        expect(payload["historical"].length).to eq(1)
        expect(payload["historical"].first["sample_date"]).to eq("2026-08-21")
        expect(payload["intraday"]).to be_nil
      end
    end

    it "writes a valid tickers.json listing the root" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        upsert_compacted(tracker, dir, provider: "schwab", root: "SPXW",
          sample_date: "2026-08-21", format: "parquet")

        described_class.new(tracker: tracker, options_dir: dir, logger: nil)
          .publish(provider: "schwab", root: "SPXW")

        payload = JSON.parse(File.read(File.join(dir, "schwab", "tickers.json")))
        expect(payload["provider"]).to eq("schwab")
        expect(payload["roots"]).to include("SPXW")
      end
    end

    it "uses remote_uri in ROOT.json for archived artifacts" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        remote = "s3://tickrake/data/options/schwab/2026/08/21/SPXW_samples_2026-08-21.parquet"
        upsert_compacted(tracker, dir, provider: "schwab", root: "SPXW",
          sample_date: "2026-08-21", format: "parquet",
          remote_uri: remote, storage_location: "ready_local_and_remote")

        described_class.new(tracker: tracker, options_dir: dir, logger: nil)
          .publish(provider: "schwab", root: "SPXW")

        payload = JSON.parse(File.read(File.join(dir, "schwab", "SPXW.json")))
        expect(payload["historical"].first["files"]["parquet"]["uri"]).to eq(remote)
      end
    end

    it "uploads ROOT.json and tickers.json to S3 when s3_archive is provided" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        upsert_compacted(tracker, dir, provider: "schwab", root: "SPXW",
          sample_date: "2026-08-21", format: "parquet")

        s3_archive = instance_double(Tickrake::Storage::S3Archive)
        uploaded_paths = []
        allow(s3_archive).to receive(:upload) { |path| uploaded_paths << path }

        described_class.new(tracker: tracker, options_dir: dir, logger: nil, s3_archive: s3_archive)
          .publish(provider: "schwab", root: "SPXW")

        expect(uploaded_paths.map { |p| File.basename(p) }).to contain_exactly("SPXW.json", "tickers.json")
      end
    end

    it "skips S3 upload when s3_archive is nil" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        upsert_compacted(tracker, dir, provider: "schwab", root: "SPXW",
          sample_date: "2026-08-21", format: "parquet")

        expect {
          described_class.new(tracker: tracker, options_dir: dir, logger: nil, s3_archive: nil)
            .publish(provider: "schwab", root: "SPXW")
        }.not_to raise_error
      end
    end
  end
end
