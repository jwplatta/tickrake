# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::IntradayPublisherJob do
  let(:logger) { instance_double(Logger, info: nil) }
  let(:candles_dir) { Dir.mktmpdir("candles") }
  let(:datastore_config) { double("datastore_config", bucket: "test-bucket") }
  let(:config) do
    instance_double(Tickrake::Config, candles_dir: candles_dir).tap do |c|
      allow(c).to receive(:datastore).with("minio_intraday").and_return(datastore_config)
    end
  end
  let(:tracker) do
    instance_double(Tickrake::Tracker,
                    intraday_active_roots: [],
                    intraday_index_rows: [],
                    intraday_series_rows: [])
  end
  let(:runtime) { instance_double(Tickrake::Runtime, config: config, logger: logger, tracker: tracker) }
  let(:scheduled_job) do
    instance_double(Tickrake::ScheduledJobConfig,
                    settings: { "datastore_name" => "minio_intraday", "clear_at" => "00:00" })
  end
  let(:store) do
    instance_double(Tickrake::Storage::S3Archive,
                    upload_file: nil, upload_content: nil,
                    list_keys: [], delete_keys: nil)
  end

  subject(:job) { described_class.new(runtime, scheduled_job: scheduled_job) }

  before do
    allow(Tickrake::Storage::S3Archive).to receive(:new).and_return(store)
  end

  after { FileUtils.rm_rf(candles_dir) }

  describe "candle publishing" do
    it "uploads candle CSVs and writes unified index" do
      provider_dir = File.join(candles_dir, "schwab", "1min")
      FileUtils.mkdir_p(provider_dir)
      File.write(File.join(provider_dir, "SPY.csv"), "datetime,open,high,low,close,volume\n2026-09-19T14:30:00Z,450,451,449,450,100000\n")

      job.run(now: Time.utc(2026, 9, 21, 14, 0, 0))

      expect(store).to have_received(:upload_file).with(
        File.join(provider_dir, "SPY.csv"),
        key: "intraday/schwab/candles/1min/SPY.csv"
      )
      expect(store).to have_received(:upload_content) do |key, content|
        expect(key).to eq("intraday/schwab/SPY.json")
        index = JSON.parse(content)
        expect(index["provider"]).to eq("schwab")
        expect(index["root"]).to eq("SPY")
        expect(index["candles"]["files"].size).to eq(1)
        expect(index["candles"]["files"].first["frequency"]).to eq("1min")
        expect(index["candles"]["files"].first["row_count"]).to eq(1)
        expect(index).not_to have_key("option_chains")
      end
    end

    it "skips empty candle CSVs (header only)" do
      provider_dir = File.join(candles_dir, "schwab", "1min")
      FileUtils.mkdir_p(provider_dir)
      File.write(File.join(provider_dir, "SPY.csv"), "datetime,open,high,low,close,volume\n")

      job.run(now: Time.utc(2026, 9, 21, 14, 0, 0))

      expect(store).not_to have_received(:upload_file)
    end

    it "evicts stale candle keys" do
      provider_dir = File.join(candles_dir, "schwab", "1min")
      FileUtils.mkdir_p(provider_dir)
      File.write(File.join(provider_dir, "SPY.csv"), "datetime,open,high,low,close,volume\n2026-09-19T14:30:00Z,450,451,449,450,100000\n")

      allow(store).to receive(:list_keys)
        .with(prefix: "intraday/schwab/candles/")
        .and_return(["intraday/schwab/candles/1min/SPY.csv", "intraday/schwab/candles/1min/OLD.csv"])

      job.run(now: Time.utc(2026, 9, 21, 14, 0, 0))

      expect(store).to have_received(:delete_keys).with(["intraday/schwab/candles/1min/OLD.csv"])
    end
  end

  describe "options time series and top-of-series publishing" do
    let(:sample_file_1) { "/tmp/options/schwab/2026/09/21/SPY_exp2026-09-21_2026-09-21_14-30-00.csv" }
    let(:sample_file_2) { "/tmp/options/schwab/2026/09/21/SPY_exp2026-09-21_2026-09-21_14-35-00.csv" }

    before do
      allow(tracker).to receive(:intraday_active_roots).and_return([{ provider_name: "schwab", root: "SPY" }])
      allow(tracker).to receive(:intraday_series_rows).with(provider_name: "schwab", root: "SPY").and_return([
        {
          "expiration_date" => "2026-09-21",
          "path" => sample_file_1,
          "row_count" => 50,
          "sample_date" => "2026-09-21",
          "sampled_at" => "2026-09-21T14:30:00Z"
        },
        {
          "expiration_date" => "2026-09-21",
          "path" => sample_file_2,
          "row_count" => 52,
          "sample_date" => "2026-09-21",
          "sampled_at" => "2026-09-21T14:35:00Z"
        }
      ])
      allow(tracker).to receive(:intraday_index_rows).with(provider_name: "schwab", root: "SPY").and_return([
        {
          "expiration_date" => "2026-09-21",
          "path" => sample_file_2,
          "row_count" => 52,
          "sample_date" => "2026-09-21",
          "sampled_at" => "2026-09-21T14:35:00Z"
        }
      ])
    end

    it "uploads time series snapshots and latest pointer, and constructs clean index" do
      job.run(now: Time.utc(2026, 9, 21, 14, 35, 0))

      # Verifies time series files uploaded with timestamp in key
      expect(store).to have_received(:upload_file).with(
        sample_file_1,
        key: "intraday/schwab/options/2026-09-21/SPY_exp2026-09-21_2026-09-21_14-30-00.csv"
      )
      expect(store).to have_received(:upload_file).with(
        sample_file_2,
        key: "intraday/schwab/options/2026-09-21/SPY_exp2026-09-21_2026-09-21_14-35-00.csv"
      )

      # Verifies top of series uploaded to latest/
      expect(store).to have_received(:upload_file).with(
        sample_file_2,
        key: "intraday/schwab/options/latest/SPY_exp2026-09-21.csv"
      )

      # Verifies index structure
      expect(store).to have_received(:upload_content).with("intraday/schwab/SPY.json", anything) do |_key, content|
        index = JSON.parse(content)
        expect(index["provider"]).to eq("schwab")
        expect(index["root"]).to eq("SPY")

        chains = index["option_chains"]
        expect(chains["sample_date"]).to eq("2026-09-21")
        expect(chains["status"]).to eq("complete")

        # Top of series
        expect(chains["latest"]["sampled_at"]).to eq("2026-09-21T14:35:00Z")
        expect(chains["latest"]["files"].size).to eq(1)
        expect(chains["latest"]["files"].first["uri"]).to eq("s3://test-bucket/intraday/schwab/options/latest/SPY_exp2026-09-21.csv")
        expect(chains["latest"]["files"].first["row_count"]).to eq(52)

        # Full time series
        expect(chains["series"].size).to eq(2)
        expect(chains["series"][0]["sampled_at"]).to eq("2026-09-21T14:30:00Z")
        expect(chains["series"][0]["uri"]).to eq("s3://test-bucket/intraday/schwab/options/2026-09-21/SPY_exp2026-09-21_2026-09-21_14-30-00.csv")
        expect(chains["series"][1]["sampled_at"]).to eq("2026-09-21T14:35:00Z")
        expect(chains["series"][1]["uri"]).to eq("s3://test-bucket/intraday/schwab/options/2026-09-21/SPY_exp2026-09-21_2026-09-21_14-35-00.csv")
      end
    end

    it "skips uploading time series files that already exist in the store (incremental upload)" do
      already_uploaded = "intraday/schwab/options/2026-09-21/SPY_exp2026-09-21_2026-09-21_14-30-00.csv"
      allow(store).to receive(:list_keys)
        .with(prefix: "intraday/schwab/options/2026-09-21/SPY_exp")
        .and_return([already_uploaded])

      job.run(now: Time.utc(2026, 9, 21, 14, 35, 0))

      expect(store).not_to have_received(:upload_file).with(sample_file_1, key: already_uploaded)
      expect(store).to have_received(:upload_file).with(sample_file_2, key: "intraday/schwab/options/2026-09-21/SPY_exp2026-09-21_2026-09-21_14-35-00.csv")
    end

    it "evicts stale keys only in latest/ and preserves active date series keys" do
      allow(store).to receive(:list_keys)
        .with(prefix: "intraday/schwab/options/latest/SPY_exp")
        .and_return(["intraday/schwab/options/latest/SPY_exp2026-09-21.csv", "intraday/schwab/options/latest/SPY_exp2026-09-20.csv"])

      job.run(now: Time.utc(2026, 9, 21, 14, 35, 0))

      expect(store).to have_received(:delete_keys).with(["intraday/schwab/options/latest/SPY_exp2026-09-20.csv"])
    end
  end

  describe "daily clear_at store eviction" do
    it "clears intraday store once daily at or after clear_at time" do
      allow(store).to receive(:list_keys).with(prefix: "intraday/").and_return([
        "intraday/schwab/options/2026-09-20/SPY_exp2026-09-20_14-30-00.csv",
        "intraday/schwab/options/latest/SPY_exp2026-09-20.csv"
      ])

      # First run at 00:01 on 2026-09-21 triggers clearance
      job.run(now: Time.utc(2026, 9, 21, 0, 1, 0))

      # Subsequent run on the same date does not clear again
      job.run(now: Time.utc(2026, 9, 21, 10, 0, 0))

      expect(store).to have_received(:delete_keys).once
    end
  end
end
