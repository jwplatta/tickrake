# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::Storage::StagingPublisher do
  let(:datastore_config) do
    {
      "type" => "s3",
      "bucket" => "tickrake-staging",
      "endpoint" => "http://localhost:9000",
      "access_key_id" => "minioadmin",
      "secret_access_key" => "minioadmin"
    }
  end

  let(:config) do
    instance_double(
      Tickrake::Config,
      data_dir: "/tmp/tickrake_data",
      staging_datastore: "minio_staging"
    ).tap do |c|
      allow(c).to receive(:datastore).with("minio_staging").and_return(datastore_config)
    end
  end
  let(:logger) { instance_double(Logger, info: nil, warn: nil, error: nil) }
  let(:runtime) { instance_double(Tickrake::Runtime, config: config, logger: logger) }
  let(:s3_archive) { instance_double(Tickrake::Storage::S3Archive) }

  let(:remote_obj) { Tickrake::Storage::S3Archive::RemoteObject.new(bucket: "tickrake-staging", key: "k", size: 10) }

  before do
    allow(Tickrake::Storage::S3Archive).to receive(:new).and_return(s3_archive)
    allow(s3_archive).to receive(:upload_file).and_return(remote_obj)
  end

  describe "#scan_and_publish" do
    it "uploads matching files, deletes local files, and emits logs" do
      Dir.mktmpdir do |dir|
        allow(Tickrake::PathSupport).to receive(:expand_path).with("/tmp/tickrake_data").and_return(dir)
        publisher = described_class.new(runtime: runtime)

        # Create dummy options and candles files
        options_dir = File.join(dir, "options", "schwab", "2026", "10", "18")
        candles_dir = File.join(dir, "candles", "daily")
        FileUtils.mkdir_p(options_dir)
        FileUtils.mkdir_p(candles_dir)

        opt_file = File.join(options_dir, "SPY_exp2026-10-18.csv")
        candle_file = File.join(candles_dir, "SPY_day.csv")
        File.write(opt_file, "strike,bid,ask\n100,1.0,1.2\n")
        File.write(candle_file, "time,open,high,low,close,volume\n")

        # Expect uploads
        expect(s3_archive).to receive(:upload_file).with(opt_file, key: "options/schwab/2026/10/18/SPY_exp2026-10-18.csv").and_return(remote_obj)
        expect(s3_archive).to receive(:upload_file).with(candle_file, key: "candles/daily/SPY_day.csv").and_return(remote_obj)

        summary = publisher.scan_and_publish(delete_after_upload: true)

        expect(summary[:files_scanned]).to eq(2)
        expect(summary[:files_uploaded]).to eq(2)
        expect(summary[:files_deleted]).to eq(2)

        expect(File.exist?(opt_file)).to be(false)
        expect(File.exist?(candle_file)).to be(false)
      end
    end

    it "preserves local files when delete_after_upload is false" do
      Dir.mktmpdir do |dir|
        allow(Tickrake::PathSupport).to receive(:expand_path).with("/tmp/tickrake_data").and_return(dir)
        publisher = described_class.new(runtime: runtime)

        level_one_dir = File.join(dir, "level_one", "schwab", "2026", "10")
        FileUtils.mkdir_p(level_one_dir)
        ndjson_file = File.join(level_one_dir, "quotes_20261018.ndjson")
        File.write(ndjson_file, "{\"symbol\":\"SPY\"}\n")

        expect(s3_archive).to receive(:upload_file).with(ndjson_file, key: "level_one/schwab/2026/10/quotes_20261018.ndjson").and_return(remote_obj)

        summary = publisher.scan_and_publish(delete_after_upload: false)

        expect(summary[:files_scanned]).to eq(1)
        expect(summary[:files_uploaded]).to eq(1)
        expect(summary[:files_deleted]).to eq(0)
        expect(File.exist?(ndjson_file)).to be(true)
      end
    end

    it "skips .tmp files" do
      Dir.mktmpdir do |dir|
        allow(Tickrake::PathSupport).to receive(:expand_path).with("/tmp/tickrake_data").and_return(dir)
        publisher = described_class.new(runtime: runtime)

        opt_dir = File.join(dir, "options")
        FileUtils.mkdir_p(opt_dir)
        tmp_file = File.join(opt_dir, "temp.csv.tmp")
        File.write(tmp_file, "temp")

        expect(s3_archive).not_to receive(:upload_file)

        summary = publisher.scan_and_publish
        expect(summary[:files_scanned]).to eq(0)
        expect(summary[:files_uploaded]).to eq(0)
      end
    end
  end
end
