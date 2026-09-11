# frozen_string_literal: true

require "spec_helper"
require "tmpdir"

RSpec.describe Tickrake::EventsIngestorJob do
  let(:tmpdir) { Dir.mktmpdir }
  let(:logger) { instance_double(Logger, info: nil, warn: nil, error: nil) }
  let(:data_dir) { tmpdir }

  let(:config) do
    instance_double(
      Tickrake::Config,
      pending_events_dir: tmpdir,
      data_dir: data_dir,
      provider_definition: nil
    )
  end

  let(:runtime) do
    instance_double(Tickrake::Runtime, config: config, logger: logger)
  end

  let(:scheduled_job) do
    instance_double(
      Tickrake::ScheduledJobConfig,
      settings: { "batch_size" => 10, "stale_age_seconds" => 1800, "datastore_name" => nil }
    )
  end

  subject(:job) { described_class.new(runtime, scheduled_job: scheduled_job) }

  after { FileUtils.rm_rf(tmpdir) }

  describe "#run" do
    context "when pending_events_dir does not exist" do
      before { FileUtils.rm_rf(tmpdir) }

      it "does nothing" do
        expect { job.run }.not_to raise_error
      end
    end

    context "when there are no .ndjson files" do
      it "does nothing" do
        expect { job.run }.not_to raise_error
      end
    end

    context "with a level_one .ndjson file" do
      let(:events) do
        [
          { "job_type" => "level_one", "provider" => "schwab", "symbol" => "SPY", "service" => "LEVELONE_EQUITIES",
            "received_at" => 1_700_000_000_000, "bid" => 1.0, "ask" => 1.1, "last" => nil,
            "bid_size" => 10, "ask_size" => 10, "volume" => 100, "mark" => nil,
            "quote_time_ms" => nil, "trade_time_ms" => nil, "extra_json" => nil }
        ]
      end

      before do
        path = File.join(tmpdir, "test_job_20260101T000000Z.ndjson")
        File.write(path, events.map { |e| JSON.generate(e) }.join("\n") + "\n")
      end

      it "deletes the staging file after processing" do
        allow_any_instance_of(Tickrake::Storage::LevelOneParquetWriter).to receive(:write)
        job.run
        expect(Dir.glob(File.join(tmpdir, "*.ndjson"))).to be_empty
      end

      it "calls LevelOneParquetWriter#write" do
        expect_any_instance_of(Tickrake::Storage::LevelOneParquetWriter).to receive(:write)
        job.run
      end
    end

    context "with an order_book .ndjson file" do
      let(:events) do
        [
          { "job_type" => "order_book", "provider" => "schwab", "symbol" => "SPY", "service" => "NYSE_BOOK",
            "received_at" => 1_700_000_000_000, "book_time_ms" => nil,
            "bids_json" => "[]", "asks_json" => "[]" }
        ]
      end

      before do
        path = File.join(tmpdir, "test_job_20260101T000000Z.ndjson")
        File.write(path, events.map { |e| JSON.generate(e) }.join("\n") + "\n")
      end

      it "calls OrderBookParquetWriter#write" do
        expect_any_instance_of(Tickrake::Storage::OrderBookParquetWriter).to receive(:write)
        job.run
      end
    end
  end

  describe "stale tmp file recovery" do
    it "renames .ndjson.tmp files older than stale_age_seconds when run is called" do
      stale_path = File.join(tmpdir, "test_job_20260101T000000Z.ndjson.tmp")
      File.write(stale_path, "{\"job_type\":null}\n")
      FileUtils.touch(stale_path, mtime: Time.now - 1900)

      job.run

      expect(File.exist?(stale_path)).to be(false)
    end

    it "leaves fresh .ndjson.tmp files alone" do
      fresh_path = File.join(tmpdir, "test_job_20260101T000000Z.ndjson.tmp")
      File.write(fresh_path, "{}\n")

      job.run

      expect(File.exist?(fresh_path)).to be(true)
    end
  end
end
