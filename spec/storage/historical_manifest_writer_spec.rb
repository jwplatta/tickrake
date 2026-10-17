# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::Storage::HistoricalManifestWriter do
  let(:store) { instance_double(Tickrake::Storage::S3Archive) }
  subject(:writer) { described_class.new(store) }

  describe "#read" do
    it "returns an empty manifest if object does not exist in store" do
      allow(store).to receive(:object_exists?).with("manifests/options.json").and_return(false)

      manifest = writer.read("options")
      expect(manifest["dataset"]).to eq("options")
      expect(manifest["file_count"]).to eq(0)
      expect(manifest["files"]).to eq([])
    end

    it "downloads and parses existing JSON manifest" do
      existing_json = JSON.generate(
        "dataset" => "options",
        "file_count" => 1,
        "files" => [{ "key" => "options/massive/file1.parquet", "row_count" => 100 }]
      )
      allow(store).to receive(:object_exists?).with("manifests/options.json").and_return(true)
      allow(store).to receive(:download_content).with("manifests/options.json").and_return(existing_json)

      manifest = writer.read("options")
      expect(manifest["file_count"]).to eq(1)
      expect(manifest["files"].first["key"]).to eq("options/massive/file1.parquet")
    end
  end

  describe "#upsert_entries" do
    it "merges new entries into existing manifest, updates totals, and uploads JSON" do
      allow(store).to receive(:object_exists?).with("manifests/candles.json").and_return(false)

      new_entries = [
        {
          key: "candles/ibkr/5min/2025/SPY.parquet",
          uri: "s3://historical/candles/ibkr/5min/2025/SPY.parquet",
          file_size: 2048,
          row_count: 500,
          checksum: "abc123",
          published_at: "2026-10-16T12:00:00Z"
        }
      ]

      uploaded_content = nil
      allow(store).to receive(:upload_content) do |key, content|
        uploaded_content = JSON.parse(content)
      end

      writer.upsert_entries("candles", new_entries)

      expect(uploaded_content["dataset"]).to eq("candles")
      expect(uploaded_content["file_count"]).to eq(1)
      expect(uploaded_content["total_rows"]).to eq(500)
      expect(uploaded_content["total_size_bytes"]).to eq(2048)
      expect(uploaded_content["files"].first["key"]).to eq("candles/ibkr/5min/2025/SPY.parquet")
    end
  end
end
