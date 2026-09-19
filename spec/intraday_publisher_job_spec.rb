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
                    intraday_index_rows: [])
  end
  let(:runtime) { instance_double(Tickrake::Runtime, config: config, logger: logger, tracker: tracker) }
  let(:scheduled_job) do
    instance_double(Tickrake::ScheduledJobConfig,
                    settings: { "datastore_name" => "minio_intraday" })
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

      job.run

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

      job.run

      expect(store).not_to have_received(:upload_file)
    end

    it "evicts stale candle keys" do
      provider_dir = File.join(candles_dir, "schwab", "1min")
      FileUtils.mkdir_p(provider_dir)
      File.write(File.join(provider_dir, "SPY.csv"), "datetime,open,high,low,close,volume\n2026-09-19T14:30:00Z,450,451,449,450,100000\n")

      allow(store).to receive(:list_keys)
        .with(prefix: "intraday/schwab/candles/")
        .and_return(["intraday/schwab/candles/1min/SPY.csv", "intraday/schwab/candles/1min/OLD.csv"])

      job.run

      expect(store).to have_received(:delete_keys).with(["intraday/schwab/candles/1min/OLD.csv"])
    end
  end

  describe "unified index with options and candles" do
    it "includes both option_chains and candles in the same index" do
      allow(tracker).to receive(:intraday_active_roots).and_return([{ provider_name: "schwab", root: "SPY" }])
      allow(tracker).to receive(:intraday_index_rows)
        .with(provider_name: "schwab", root: "SPY")
        .and_return([{
          "expiration_date" => "2026-09-20",
          "path" => "/tmp/spy_exp.csv",
          "row_count" => 50,
          "sample_date" => "2026-09-19",
          "sampled_at" => "2026-09-19T14:30:00Z"
        }])
      allow(store).to receive(:list_keys).with(prefix: "intraday/schwab/options/SPY_exp").and_return([])

      provider_dir = File.join(candles_dir, "schwab", "1min")
      FileUtils.mkdir_p(provider_dir)
      File.write(File.join(provider_dir, "SPY.csv"), "datetime,open,high,low,close,volume\n2026-09-19T14:30:00Z,450,451,449,450,100000\n")
      allow(store).to receive(:list_keys).with(prefix: "intraday/schwab/candles/").and_return([])

      job.run

      expect(store).to have_received(:upload_content).with("intraday/schwab/SPY.json", anything) do |_key, content|
        index = JSON.parse(content)
        expect(index).to have_key("option_chains")
        expect(index).to have_key("candles")
        expect(index["option_chains"]["files"].first["expiration_date"]).to eq("2026-09-20")
        expect(index["candles"]["files"].first["frequency"]).to eq("1min")
      end
    end
  end

  describe "options-only publishing" do
    it "uses option_chains key instead of intraday" do
      allow(tracker).to receive(:intraday_active_roots).and_return([{ provider_name: "schwab", root: "AAPL" }])
      allow(tracker).to receive(:intraday_index_rows)
        .with(provider_name: "schwab", root: "AAPL")
        .and_return([{
          "expiration_date" => "2026-09-20",
          "path" => "/tmp/aapl_exp.csv",
          "row_count" => 25,
          "sample_date" => "2026-09-19",
          "sampled_at" => "2026-09-19T14:30:00Z"
        }])
      allow(store).to receive(:list_keys).with(prefix: "intraday/schwab/options/AAPL_exp").and_return([])

      job.run

      expect(store).to have_received(:upload_content).with("intraday/schwab/AAPL.json", anything) do |_key, content|
        index = JSON.parse(content)
        expect(index).to have_key("option_chains")
        expect(index).not_to have_key("intraday")
        expect(index).not_to have_key("candles")
      end
    end
  end
end
