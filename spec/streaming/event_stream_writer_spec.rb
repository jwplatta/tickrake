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
end
