# frozen_string_literal: true

require "spec_helper"
require "tmpdir"

RSpec.describe Tickrake::FundamentalsJob do
  let(:tmpdir) { Dir.mktmpdir }
  let(:logger) { instance_double(Logger, info: nil, warn: nil, error: nil) }
  let(:provider_def) do
    double("provider_def", rate_limit_max_requests: nil, rate_limit_interval_seconds: nil)
  end
  let(:config) do
    instance_double(
      Tickrake::Config,
      data_dir: tmpdir,
      pending_metadata_dir: File.join(tmpdir, "pending_metadata"),
      provider_definition: provider_def
    )
  end
  let(:client) { instance_double("client") }
  let(:client_factory) { instance_double(Tickrake::ClientFactory, build: client) }
  let(:runtime) do
    instance_double(
      Tickrake::Runtime,
      config: config,
      client_factory: client_factory,
      logger: logger
    )
  end
  let(:scheduled_job) do
    double(
      "scheduled_job",
      provider: "schwab",
      name: "fundamentals_job",
      universe: ["AAPL"],
      settings: {}
    )
  end
  let(:job) { described_class.new(runtime, scheduled_job: scheduled_job) }

  after { FileUtils.rm_rf(tmpdir) }

  describe "#run" do
    let(:instrument_data) do
      {
        instruments: [
          {
            symbol: "AAPL",
            assetType: "EQUITY",
            exchange: "NASDAQ",
            description: "Apple Inc",
            fundamental: {
              symbol: "AAPL",
              high52: 200.0,
              low52: 150.0,
              peRatio: 30.5
            }
          }
        ]
      }
    end

    before do
      allow(client).to receive(:get_instruments).and_return(instrument_data)
      allow(client).to receive(:get_quotes).and_return({})
    end

    it "writes parquet file and emits parquet_written with byte_count and storage_byte_delta" do
      logged = []
      allow(logger).to receive(:info) { |payload| logged << payload }

      result = job.run(now: Time.utc(2026, 9, 20, 12, 0, 0))
      expect(result.successful?).to be(true)

      event = logged.find { |l| l.is_a?(Hash) && l[:event] == "parquet_written" }
      expect(event).not_to be_nil
      expect(event[:data_type]).to eq("fundamentals")
      expect(event[:provider]).to eq("schwab")
      expect(event[:market_date]).to eq("2026-09-20")
      expect(event[:row_count]).to eq(1)
      expect(event[:path]).to be_a(String)
      expect(event[:byte_count]).to eq(File.size(event[:path]))
      expect(event[:byte_count]).to be > 0
      expect(event[:storage_byte_delta]).to eq(event[:byte_count])

      # Running again on same day produces 0 delta
      logged.clear
      job.run(now: Time.utc(2026, 9, 20, 12, 0, 0))
      second_event = logged.find { |l| l.is_a?(Hash) && l[:event] == "parquet_written" }
      expect(second_event).not_to be_nil
      expect(second_event[:storage_byte_delta]).to eq(0)
    end

    it "does not emit parquet_written event if parquet writing fails" do
      logged = []
      allow(logger).to receive(:info) { |payload| logged << payload }
      allow_any_instance_of(Tickrake::Storage::FundamentalsParquetWriter).to receive(:write).and_raise(IOError, "write error")

      result = job.run(now: Time.utc(2026, 9, 20, 12, 0, 0))
      expect(result.successful?).to be(false)
      expect(logged.select { |l| l.is_a?(Hash) && l[:event] == "parquet_written" }).to be_empty
    end
  end
end
