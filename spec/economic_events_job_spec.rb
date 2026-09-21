# frozen_string_literal: true

require "spec_helper"
require "tmpdir"

RSpec.describe Tickrake::EconomicEventsJob do
  let(:tmpdir) { Dir.mktmpdir }
  let(:logger) { instance_double(Logger, info: nil, warn: nil, error: nil) }
  let(:config) { instance_double(Tickrake::Config, data_dir: tmpdir) }
  let(:runtime) { instance_double(Tickrake::Runtime, config: config, logger: logger) }

  let(:scheduled_job) do
    instance_double(Tickrake::ScheduledJobConfig, settings: {
      "lookback_days" => 7, "lookahead_days" => 14,
      "categories" => %w[economic earnings fomc]
    })
  end

  subject(:job) { described_class.new(runtime, scheduled_job: scheduled_job) }

  after { FileUtils.rm_rf(tmpdir) }

  def stub_fetcher(klass, rows)
    dbl = instance_double(klass, fetch: rows)
    allow(klass).to receive(:new).and_return(dbl)
    dbl
  end

  let(:bls_row)      { { event_datetime: "2026-10-14T12:30:00Z", event_name: "CPI",               category: "economic", source: "bls",           actual: nil, estimate: nil, previous: nil, unit: nil, symbol: nil, company_name: nil, time_of_day: nil, release_id: nil, series_id: nil, fetched_at: "2026-09-20T00:00:00Z" } }
  let(:fred_row)     { { event_datetime: "2026-09-25T12:30:00Z", event_name: "GDP",               category: "economic", source: "fred",          actual: nil, estimate: nil, previous: nil, unit: nil, symbol: nil, company_name: nil, time_of_day: nil, release_id: 53,  series_id: "GDP", fetched_at: "2026-09-20T00:00:00Z" } }
  let(:fomc_row)     { { event_datetime: "2026-09-17T18:00:00Z", event_name: "FOMC Rate Decision", category: "fomc",     source: "fred",          actual: 5.25, estimate: nil, previous: nil, unit: "percent", symbol: nil, company_name: nil, time_of_day: nil, release_id: 21, series_id: "DFEDTARU", fetched_at: "2026-09-20T00:00:00Z" } }
  let(:earnings_row) { { event_datetime: "2026-10-15T00:00:00Z", event_name: "Earnings",          category: "earnings", source: "alpha_vantage", actual: nil, estimate: 1.52, previous: nil, unit: "USD", symbol: "AAPL", company_name: "Apple Inc", time_of_day: "post-market", release_id: nil, series_id: nil, fetched_at: "2026-09-20T00:00:00Z" } }

  before do
    stub_fetcher(Tickrake::Fetchers::BlsCalendar,           [bls_row])
    stub_fetcher(Tickrake::Fetchers::FredEconomicCalendar,  [fred_row])
    stub_fetcher(Tickrake::Fetchers::FomcCalendar,          [fomc_row])
    stub_fetcher(Tickrake::Fetchers::AlphaVantageEarnings,  [earnings_row])
  end

  describe "#run" do
    it "writes parquet files for each source/category/date" do
      job.run(now: Time.utc(2026, 9, 20))
      expect(File).to exist(File.join(tmpdir, "economic_events/bls/economic/2026/10/14.parquet"))
      expect(File).to exist(File.join(tmpdir, "economic_events/fred/economic/2026/09/25.parquet"))
      expect(File).to exist(File.join(tmpdir, "economic_events/fred/fomc/2026/09/17.parquet"))
      expect(File).to exist(File.join(tmpdir, "economic_events/alpha_vantage/earnings/2026/10/15.parquet"))
    end

    it "returns a successful result" do
      result = job.run(now: Time.utc(2026, 9, 20))
      expect(result.successful?).to be(true)
    end

    it "continues writing other sources when one fetcher raises" do
      allow(Tickrake::Fetchers::BlsCalendar).to receive(:new).and_return(
        instance_double(Tickrake::Fetchers::BlsCalendar, fetch: nil).tap do |d|
          allow(d).to receive(:fetch).and_raise(Tickrake::Error, "network error")
        end
      )
      job.run(now: Time.utc(2026, 9, 20))
      expect(File).to exist(File.join(tmpdir, "economic_events/fred/economic/2026/09/25.parquet"))
      expect(logger).to have_received(:error).at_least(:once)
    end

    it "skips earnings when not in categories" do
      allow(scheduled_job).to receive(:settings).and_return(
        "lookback_days" => 7, "lookahead_days" => 14, "categories" => %w[economic fomc]
      )
      job.run(now: Time.utc(2026, 9, 20))
      expect(File).not_to exist(File.join(tmpdir, "economic_events/alpha_vantage/earnings/2026/10/15.parquet"))
    end
  end
end
