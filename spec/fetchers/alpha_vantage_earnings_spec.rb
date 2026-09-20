# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::Fetchers::AlphaVantageEarnings do
  subject(:fetcher) { described_class.new }

  before { stub_const("ENV", ENV.to_h.merge("ALPHA_VANTAGE_API_KEY" => "testkey")) }

  let(:csv_body) { <<~CSV }
    symbol,name,reportDate,fiscalDateEnding,estimate,currency,reportTime
    AAPL,Apple Inc,2026-10-15,2026-09-30,1.52,USD,post-market
    MSFT,Microsoft Corp,2026-10-23,2026-09-30,3.10,USD,post-market
    GOOG,Alphabet Inc,2026-10-28,2026-09-30,,USD,after-hours
  CSV

  before do
    response = instance_double(Net::HTTPSuccess, body: csv_body.b)
    allow(response).to receive(:is_a?).with(Net::HTTPSuccess).and_return(true)
    allow(Net::HTTP).to receive(:get_response).and_return(response)
  end

  describe "#fetch" do
    let(:from_date) { Date.new(2026, 10, 1) }
    let(:to_date)   { Date.new(2026, 10, 31) }

    it "returns rows within the window" do
      rows = fetcher.fetch(from_date: from_date, to_date: to_date)
      expect(rows.length).to eq(3)
    end

    it "excludes rows outside the window" do
      rows = fetcher.fetch(from_date: Date.new(2026, 10, 20), to_date: Date.new(2026, 10, 31))
      expect(rows.map { |r| r[:symbol] }).to contain_exactly("MSFT", "GOOG")
    end

    it "parses symbol, company_name, and estimate" do
      rows = fetcher.fetch(from_date: from_date, to_date: to_date)
      aapl = rows.find { |r| r[:symbol] == "AAPL" }
      expect(aapl[:company_name]).to eq("Apple Inc")
      expect(aapl[:estimate]).to eq(1.52)
    end

    it "sets nil estimate when field is blank" do
      rows = fetcher.fetch(from_date: from_date, to_date: to_date)
      goog = rows.find { |r| r[:symbol] == "GOOG" }
      expect(goog[:estimate]).to be_nil
    end

    it "sets event_datetime as midnight UTC on the report date" do
      rows = fetcher.fetch(from_date: from_date, to_date: to_date)
      aapl = rows.find { |r| r[:symbol] == "AAPL" }
      expect(aapl[:event_datetime]).to eq("2026-10-15T00:00:00Z")
    end

    it "sets source=alpha_vantage, category=earnings" do
      rows = fetcher.fetch(from_date: from_date, to_date: to_date)
      expect(rows).to all(include(source: "alpha_vantage", category: "earnings"))
    end

    it "raises ConfigError when API key is missing" do
      stub_const("ENV", ENV.to_h.reject { |k, _| k == "ALPHA_VANTAGE_API_KEY" })
      expect { described_class.new }.to raise_error(Tickrake::ConfigError, /ALPHA_VANTAGE_API_KEY/)
    end

    it "raises when HTTP response is not success" do
      bad = instance_double(Net::HTTPClientError)
      allow(bad).to receive(:is_a?).with(Net::HTTPSuccess).and_return(false)
      allow(bad).to receive(:code).and_return("403")
      allow(bad).to receive(:body).and_return("Forbidden")
      allow(Net::HTTP).to receive(:get_response).and_return(bad)

      expect { fetcher.fetch(from_date: from_date, to_date: to_date) }.to raise_error(Tickrake::Error, /403/)
    end
  end
end
