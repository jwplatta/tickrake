# frozen_string_literal: true

require "spec_helper"
require "tmpdir"

RSpec.describe Tickrake::EconomicEventsJob do
  let(:tmpdir) { Dir.mktmpdir }
  let(:logger) { instance_double(Logger, info: nil, warn: nil, error: nil) }

  let(:config) do
    instance_double(
      Tickrake::Config,
      data_dir: tmpdir
    )
  end

  let(:runtime) do
    instance_double(Tickrake::Runtime, config: config, logger: logger)
  end

  let(:scheduled_job) do
    instance_double(
      Tickrake::ScheduledJobConfig,
      settings: {
        "lookback_days"  => 7,
        "lookahead_days" => 14,
        "categories"     => %w[economic earnings fomc]
      }
    )
  end

  subject(:job) { described_class.new(runtime, scheduled_job: scheduled_job) }

  after { FileUtils.rm_rf(tmpdir) }

  let(:bls_row) do
    {
      event_datetime: "2026-10-14T12:30:00Z",
      event_name: "Consumer Price Index",
      category: "economic",
      source: "bls",
      actual: nil, estimate: nil, previous: nil, unit: nil,
      symbol: nil, company_name: nil, time_of_day: nil,
      release_id: nil, series_id: nil, fetched_at: "2026-09-20T00:00:00Z"
    }
  end

  let(:fred_row) do
    {
      event_datetime: "2026-09-25T12:30:00Z",
      event_name: "GDP",
      category: "economic",
      source: "fred",
      actual: 23_000.5, estimate: nil, previous: nil, unit: nil,
      symbol: nil, company_name: nil, time_of_day: nil,
      release_id: 53, series_id: "GDP", fetched_at: "2026-09-20T00:00:00Z"
    }
  end

  let(:fomc_row) do
    {
      event_datetime: "2026-09-17T18:00:00Z",
      event_name: "FOMC Rate Decision",
      category: "fomc",
      source: "fred",
      actual: 5.25, estimate: nil, previous: nil, unit: "percent",
      symbol: nil, company_name: nil, time_of_day: nil,
      release_id: 21, series_id: "DFEDTARU", fetched_at: "2026-09-20T00:00:00Z"
    }
  end

  let(:earnings_row) do
    {
      event_datetime: "2026-10-15T00:00:00Z",
      event_name: "Earnings",
      category: "earnings",
      source: "alpha_vantage",
      actual: nil, estimate: 1.52, previous: nil, unit: "USD",
      symbol: "AAPL", company_name: "Apple Inc", time_of_day: "post-market",
      release_id: nil, series_id: nil, fetched_at: "2026-09-20T00:00:00Z"
    }
  end

  before do
    bls = instance_double(Tickrake::Fetchers::BlsCalendar, fetch: [bls_row])
    allow(Tickrake::Fetchers::BlsCalendar).to receive(:new).and_return(bls)

    fred = instance_double(Tickrake::Fetchers::FredEconomicCalendar, fetch: [fred_row])
    allow(Tickrake::Fetchers::FredEconomicCalendar).to receive(:new).and_return(fred)

    fomc = instance_double(Tickrake::Fetchers::FomcCalendar, fetch: [fomc_row])
    allow(Tickrake::Fetchers::FomcCalendar).to receive(:new).and_return(fomc)

    earnings = instance_double(Tickrake::Fetchers::AlphaVantageEarnings, fetch: [earnings_row])
    allow(Tickrake::Fetchers::AlphaVantageEarnings).to receive(:new).and_return(earnings)
  end

  describe "#run" do
    it "returns a successful ScheduledRunResult" do
      result = job.run(now: Time.utc(2026, 9, 20))
      expect(result.successful?).to be(true)
    end

    it "writes parquet files for each source/category/date" do
      job.run(now: Time.utc(2026, 9, 20))

      expect(File).to exist(File.join(tmpdir, "economic_events/bls/economic/2026/10/14.parquet"))
      expect(File).to exist(File.join(tmpdir, "economic_events/fred/economic/2026/09/25.parquet"))
      expect(File).to exist(File.join(tmpdir, "economic_events/fred/fomc/2026/09/17.parquet"))
      expect(File).to exist(File.join(tmpdir, "economic_events/alpha_vantage/earnings/2026/10/15.parquet"))
    end

    it "returns success_count equal to total rows written" do
      result = job.run(now: Time.utc(2026, 9, 20))
      expect(result.success_count).to eq(4)
    end

    context "when a fetcher raises" do
      before do
        bls = instance_double(Tickrake::Fetchers::BlsCalendar)
        allow(bls).to receive(:fetch).and_raise(Tickrake::Error, "network error")
        allow(Tickrake::Fetchers::BlsCalendar).to receive(:new).and_return(bls)
      end

      it "continues and still writes other sources" do
        job.run(now: Time.utc(2026, 9, 20))
        expect(File).to exist(File.join(tmpdir, "economic_events/fred/economic/2026/09/25.parquet"))
      end

      it "logs the error" do
        job.run(now: Time.utc(2026, 9, 20))
        expect(logger).to have_received(:error).at_least(:once)
      end
    end

    context "when categories excludes earnings" do
      let(:scheduled_job) do
        instance_double(
          Tickrake::ScheduledJobConfig,
          settings: { "lookback_days" => 7, "lookahead_days" => 14, "categories" => %w[economic fomc] }
        )
      end

      it "does not write earnings files" do
        job.run(now: Time.utc(2026, 9, 20))
        expect(File).not_to exist(File.join(tmpdir, "economic_events/alpha_vantage/earnings/2026/10/15.parquet"))
      end
    end
  end
end
