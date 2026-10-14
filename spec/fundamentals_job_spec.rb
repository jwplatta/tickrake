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

    it "captures expanded fundamental, dividend, borrow, and quote fields in the generated parquet file" do
      inst_payload = {
        instruments: [
          {
            symbol: "AAPL",
            assetType: "EQUITY",
            exchange: "NASDAQ",
            description: "Apple Inc",
            cusip: "037833100",
            fundamental: {
              symbol: "AAPL",
              high52: 200.0,
              low52: 150.0,
              peRatio: 30.5,
              epsChange: 1.25,
              epsChangeYear: 12.5,
              revChangeIn: 8.4,
              fundLeverageFactor: 100.0,
              declarationDate: "2026-08-01 00:00:00.0",
              nextDividendDate: "2026-11-06 00:00:00.0",
              nextDividendPayDate: "2026-11-13 00:00:00.0",
              dividendAmount: 1.0,
              dividendYield: 0.5
            }
          }
        ]
      }
      quotes_payload = {
        "AAPL" => {
          fundamental: {
            lastEarningsDate: "2026-08-01T00:00:00Z",
            avg1YearVolume: 55_000_000.0
          },
          reference: {
            cusip: "037833100",
            isHardToBorrow: false,
            isShortable: true,
            htbQuantity: 1_000_000,
            htbRate: 0.25
          },
          quote: {
            closePrice: 195.5,
            lastPrice: 196.0,
            netChange: 1.5,
            netPercentChange: 0.77,
            totalVolume: 42_000_000
          }
        }
      }

      allow(client).to receive(:get_instruments).and_return(inst_payload)
      allow(client).to receive(:get_quotes).and_return(quotes_payload)

      result = job.run(now: Time.utc(2026, 9, 20, 12, 0, 0))
      expect(result.successful?).to be(true)

      expected_path = File.join(tmpdir, "fundamentals", "schwab", "2026", "09", "20.parquet")
      expect(File.exist?(expected_path)).to be(true)

      rows = Parquet.each_row(expected_path).to_a
      expect(rows.length).to eq(1)
      row = rows.first

      expect(row["symbol"]).to eq("AAPL")
      expect(row["cusip"]).to eq("037833100")
      expect(row["eps_change"]).to eq(1.25)
      expect(row["eps_change_year"]).to eq(12.5)
      expect(row["rev_change_in"]).to eq(8.4)
      expect(row["fund_leverage_factor"]).to eq(100.0)
      expect(row["is_hard_to_borrow"]).to eq(false)
      expect(row["is_shortable"]).to eq(true)
      expect(row["htb_quantity"]).to eq(1_000_000)
      expect(row["htb_rate"]).to eq(0.25)
      expect(row["declaration_date"]).to eq("2026-08-01 00:00:00.0")
      expect(row["next_div_ex_date"]).to eq("2026-11-06 00:00:00.0")
      expect(row["next_div_pay_date"]).to eq("2026-11-13 00:00:00.0")
      expect(row["close_price"]).to eq(195.5)
      expect(row["last_price"]).to eq(196.0)
      expect(row["net_change"]).to eq(1.5)
      expect(row["net_percent_change"]).to eq(0.77)
      expect(row["total_volume"]).to eq(42_000_000)
      expect(row["avg_1_year_volume"]).to eq(55_000_000)
      expect(row["last_earnings_date"]).to eq("2026-08-01T00:00:00Z")
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
