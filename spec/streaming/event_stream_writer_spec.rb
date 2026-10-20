# frozen_string_literal: true

require "spec_helper"
require "tmpdir"

RSpec.describe Tickrake::Streaming::EventStreamWriter do
  let(:tmpdir) { Dir.mktmpdir }
  let(:logger) { instance_double(Logger, info: nil, warn: nil, error: nil) }
  let(:provider_def) do
    instance_double(
      Tickrake::ProviderDefinition,
      symbol_map: {
        "/ES" => "^ES",
        "/NQ" => "^NQ"
      }
    )
  end

  let(:config) do
    instance_double(
      Tickrake::Config,
      data_dir: tmpdir
    )
  end

  after { FileUtils.rm_rf(tmpdir) }

  describe "level_one streaming writes" do
    subject(:writer) do
      described_class.new(
        config: config,
        logger: logger,
        dataset_type: "level_one",
        provider: "schwab",
        provider_definition: provider_def
      )
    end

    it "appends events directly to per-symbol daily NDJSON files" do
      writer.write("symbol" => "SPY", "bid" => 580.1)
      writer.write("symbol" => "SPY", "bid" => 580.2)
      writer.write("symbol" => "QQQ", "bid" => 490.5)
      writer.close

      today = Time.now.utc
      date_path = today.strftime("%Y/%m/%d")

      spy_file = File.join(tmpdir, "level_one", "schwab", date_path, "SPY.ndjson")
      qqq_file = File.join(tmpdir, "level_one", "schwab", date_path, "QQQ.ndjson")

      expect(File.exist?(spy_file)).to be(true)
      expect(File.exist?(qqq_file)).to be(true)

      spy_lines = File.readlines(spy_file).map { |l| JSON.parse(l) }
      expect(spy_lines.size).to eq(2)
      expect(spy_lines.map { |l| l["bid"] }).to eq([580.1, 580.2])

      qqq_lines = File.readlines(qqq_file).map { |l| JSON.parse(l) }
      expect(qqq_lines.size).to eq(1)
      expect(qqq_lines.first["bid"]).to eq(490.5)
    end

    it "normalizes futures symbols using provider symbol_map" do
      writer.write("symbol" => "/ES", "bid" => 5800.0)
      writer.close

      today = Time.now.utc
      date_path = today.strftime("%Y/%m/%d")

      # /ES is mapped to ^ES in symbol_map, and storage_token preserves ^
      es_file = File.join(tmpdir, "level_one", "schwab", date_path, "^ES.ndjson")
      expect(File.exist?(es_file)).to be(true)

      line = JSON.parse(File.read(es_file).strip)
      expect(line["symbol"]).to eq("/ES")
      expect(line["bid"]).to eq(5800.0)
    end
  end

  describe "order_book streaming writes" do
    subject(:writer) do
      described_class.new(
        config: config,
        logger: logger,
        dataset_type: "order_book",
        provider: "schwab",
        provider_definition: provider_def
      )
    end

    it "appends order book events directly to data/order_book/..." do
      writer.write("symbol" => "SPY", "bids_json" => "[]", "asks_json" => "[]")
      writer.close

      today = Time.now.utc
      date_path = today.strftime("%Y/%m/%d")

      spy_file = File.join(tmpdir, "order_book", "schwab", date_path, "SPY.ndjson")
      expect(File.exist?(spy_file)).to be(true)

      line = JSON.parse(File.read(spy_file).strip)
      expect(line["symbol"]).to eq("SPY")
      expect(line["bids_json"]).to eq("[]")
    end
  end

  describe "intraday byte tracking and progress logging" do
    let(:logged_events) { [] }
    let(:test_logger) do
      l = instance_double(Logger)
      allow(l).to receive(:info) { |payload| logged_events << payload }
      allow(l).to receive(:warn)
      allow(l).to receive(:error)
      l
    end

    subject(:writer) do
      described_class.new(
        config: config,
        logger: test_logger,
        dataset_type: "level_one",
        provider: "schwab",
        provider_definition: provider_def,
        log_interval: 10 # 10 seconds
      )
    end

    it "tracks byte count and emits structured stream_bytes_written events on close and on interval" do
      writer.write("symbol" => "SPY", "bid" => 580.1)
      writer.write("symbol" => "SPY", "bid" => 580.2)

      today_str = Time.now.utc.strftime("%Y-%m-%d")
      spy_stat = writer.stats[[today_str, "SPY"]]
      expect(spy_stat[:event_count]).to eq(2)
      expect(spy_stat[:byte_count]).to be > 0

      # Flushing with force logs all pending bytes
      writer.flush_progress(force: true)
      expect(logged_events.size).to eq(1)

      log_evt = logged_events.first
      expect(log_evt[:event]).to eq("stream_bytes_written")
      expect(log_evt[:symbol]).to eq("SPY")
      expect(log_evt[:dataset_type]).to eq("level_one")
      expect(log_evt[:provider]).to eq("schwab")
      expect(log_evt[:market_date]).to eq(today_str)
      expect(log_evt[:byte_count]).to eq(spy_stat[:byte_count])
      expect(log_evt[:delta_bytes]).to eq(spy_stat[:byte_count])
      expect(log_evt[:event_count]).to eq(2)
      expect(log_evt[:delta_events]).to eq(2)

      # Second write and close flushes remaining delta
      writer.write("symbol" => "SPY", "bid" => 580.3)
      writer.close

      expect(logged_events.size).to eq(2)
      second_evt = logged_events.last
      expect(second_evt[:event_count]).to eq(3)
      expect(second_evt[:delta_events]).to eq(1)
      expect(second_evt[:byte_count]).to be > log_evt[:byte_count]
      expect(second_evt[:delta_bytes]).to eq(second_evt[:byte_count] - log_evt[:byte_count])
    end
  end
end
