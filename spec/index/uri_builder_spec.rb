# frozen_string_literal: true

require_relative "../spec_helper"

RSpec.describe Tickrake::Index::UriBuilder do
  describe ".build" do
    it "returns the remote_uri for ready_local_and_remote artifacts" do
      uri = described_class.build(
        path: "/home/tickrake/.tickrake/data/options/schwab/2026/08/21/SPXW_samples_2026-08-21.parquet",
        storage_location: "ready_local_and_remote",
        remote_uri: "s3://tickrake/data/options/schwab/2026/08/21/SPXW_samples_2026-08-21.parquet"
      )
      expect(uri).to eq("s3://tickrake/data/options/schwab/2026/08/21/SPXW_samples_2026-08-21.parquet")
    end

    it "returns the remote_uri for remote-only artifacts" do
      uri = described_class.build(
        path: "/home/tickrake/.tickrake/data/options/schwab/2026/08/21/SPXW_samples_2026-08-21.parquet",
        storage_location: "remote",
        remote_uri: "s3://tickrake/data/options/schwab/2026/08/21/SPXW_samples_2026-08-21.parquet"
      )
      expect(uri).to eq("s3://tickrake/data/options/schwab/2026/08/21/SPXW_samples_2026-08-21.parquet")
    end

    it "returns a file:// URI for local-only artifacts" do
      uri = described_class.build(
        path: "/home/tickrake/.tickrake/data/options/schwab/2026/08/23/SPXW_exp2026-08-23_2026-08-23_15-42-10.csv",
        storage_location: "local",
        remote_uri: nil
      )
      expect(uri).to eq("file:///home/tickrake/.tickrake/data/options/schwab/2026/08/23/SPXW_exp2026-08-23_2026-08-23_15-42-10.csv")
    end

    it "falls back to file:// when remote_uri is nil even if storage_location is not local" do
      uri = described_class.build(
        path: "/some/path/file.parquet",
        storage_location: "ready_local_and_remote",
        remote_uri: nil
      )
      expect(uri).to eq("file:///some/path/file.parquet")
    end
  end
end
