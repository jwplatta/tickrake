# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::ChartStreamJob do
  let(:logger) { instance_double(Logger, info: nil, error: nil) }
  let(:candles_dir) { Dir.mktmpdir("candles") }
  let(:config) do
    instance_double(Tickrake::Config, candles_dir: candles_dir).tap do |c|
      allow(c).to receive(:option_snapshot_filename_timezone).and_return("UTC")
    end
  end
  let(:runtime) do
    instance_double(Tickrake::Runtime, config: config, logger: logger)
  end
  let(:chart_stream_config) do
    Tickrake::ChartStreamConfig.new(services: [:chart_equity], flush_interval_seconds: 60)
  end
  let(:scheduled_job) do
    instance_double(Tickrake::ScheduledJobConfig,
                    name: "test_chart",
                    provider: "schwab",
                    settings: chart_stream_config,
                    universe: ["SPY"])
  end

  subject(:job) { described_class.new(runtime, scheduled_job: scheduled_job) }

  after { FileUtils.rm_rf(candles_dir) }

  describe "#handle_event" do
    it "buffers bars from CHART_EQUITY events" do
      event = {
        "content" => [{
          "key" => "SPY",
          "1" => 450.0,
          "2" => 451.0,
          "3" => 449.5,
          "4" => 450.5,
          "5" => 100_000,
          "6" => 1,
          "7" => 1_695_000_000_000
        }]
      }

      job.send(:handle_event, event, service: "CHART_EQUITY")

      buffer = job.instance_variable_get(:@buffer)
      expect(buffer["SPY"].size).to eq(1)

      bar = buffer["SPY"].first
      expect(bar.open).to eq(450.0)
      expect(bar.high).to eq(451.0)
      expect(bar.low).to eq(449.5)
      expect(bar.close).to eq(450.5)
      expect(bar.volume).to eq(100_000)
      expect(bar.symbol).to eq("SPY")
      expect(bar.frequency).to eq("1min")
    end

    it "buffers bars from CHART_FUTURES events" do
      event = {
        "content" => [{
          "key" => "/ES",
          "1" => 1_695_000_000_000,
          "2" => 4500.0,
          "3" => 4510.0,
          "4" => 4495.0,
          "5" => 4505.0,
          "6" => 50_000
        }]
      }

      job.send(:handle_event, event, service: "CHART_FUTURES")

      buffer = job.instance_variable_get(:@buffer)
      expect(buffer["/ES"].size).to eq(1)

      bar = buffer["/ES"].first
      expect(bar.open).to eq(4500.0)
      expect(bar.high).to eq(4510.0)
      expect(bar.close).to eq(4505.0)
      expect(bar.volume).to eq(50_000)
    end

    it "skips entries without chart_time_ms" do
      event = { "content" => [{ "key" => "SPY", "1" => 450.0 }] }
      job.send(:handle_event, event, service: "CHART_EQUITY")

      buffer = job.instance_variable_get(:@buffer)
      expect(buffer).to be_empty
    end
  end

  describe "#flush_buffer" do
    let(:symbol_normalizer) { Tickrake::Query::SymbolNormalizer.new }

    it "appends bars to a CSV file" do
      bar = Tickrake::Data::Bar.new(
        datetime: Time.utc(2026, 9, 19, 14, 30, 0),
        open: 450.0, high: 451.0, low: 449.5, close: 450.5, volume: 100_000,
        source: "schwab", symbol: "SPY", frequency: "1min"
      )

      job.instance_variable_get(:@buffer)["SPY"] = [bar]
      job.send(:flush_buffer)

      path = File.join(candles_dir, "schwab", "1min", "SPY.csv")
      expect(File.exist?(path)).to be true

      lines = File.readlines(path)
      expect(lines[0].strip).to eq("datetime,open,high,low,close,volume")
      expect(lines[1]).to include("450.0")
      expect(lines[1]).to include("100000")
    end

    it "appends without rewriting headers on second flush" do
      2.times do |i|
        bar = Tickrake::Data::Bar.new(
          datetime: Time.utc(2026, 9, 19, 14, 30 + i, 0),
          open: 450.0, high: 451.0, low: 449.5, close: 450.5, volume: 100_000,
          source: "schwab", symbol: "SPY", frequency: "1min"
        )
        job.instance_variable_get(:@buffer)["SPY"] = [bar]
        job.send(:flush_buffer)
      end

      path = File.join(candles_dir, "schwab", "1min", "SPY.csv")
      lines = File.readlines(path)
      expect(lines.size).to eq(3) # header + 2 data rows
    end

    it "clears the buffer after flush" do
      bar = Tickrake::Data::Bar.new(
        datetime: Time.utc(2026, 9, 19, 14, 30, 0),
        open: 450.0, high: 451.0, low: 449.5, close: 450.5, volume: 100_000,
        source: "schwab", symbol: "SPY", frequency: "1min"
      )
      job.instance_variable_get(:@buffer)["SPY"] = [bar]
      job.send(:flush_buffer)

      expect(job.instance_variable_get(:@buffer)).to be_empty
    end
  end
end
