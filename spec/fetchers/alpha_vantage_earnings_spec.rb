# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::Fetchers::AlphaVantageEarnings do
  before { stub_const("ENV", ENV.to_h.merge("ALPHA_VANTAGE_API_KEY" => "testkey")) }

  subject(:fetcher) { described_class.new }

  let(:csv_body) { <<~CSV }
    symbol,name,reportDate,fiscalDateEnding,estimate,currency,reportTime
    AAPL,Apple Inc,2026-10-15,2026-09-30,1.52,USD,post-market
    MSFT,Microsoft Corp,2026-10-23,2026-09-30,3.10,USD,post-market
  CSV

  before do
    response = instance_double(Net::HTTPSuccess, body: csv_body.b)
    allow(response).to receive(:is_a?).with(Net::HTTPSuccess).and_return(true)
    allow(Net::HTTP).to receive(:get_response).and_return(response)
  end

  describe "#fetch" do
    it "filters to window and parses symbol, estimate, and midnight UTC datetime" do
      rows = fetcher.fetch(from_date: Date.new(2026, 10, 20), to_date: Date.new(2026, 10, 31))
      expect(rows.map { |r| r[:symbol] }).to eq(["MSFT"])
      expect(rows.first[:estimate]).to eq(3.10)
      expect(rows.first[:event_datetime]).to eq("2026-10-23T00:00:00Z")
    end

    it "raises ConfigError when API key is missing" do
      stub_const("ENV", ENV.to_h.reject { |k, _| k == "ALPHA_VANTAGE_API_KEY" })
      expect { described_class.new }.to raise_error(Tickrake::ConfigError, /ALPHA_VANTAGE_API_KEY/)
    end

    it "raises on HTTP error" do
      bad = instance_double(Net::HTTPClientError, code: "403", body: "Forbidden")
      allow(bad).to receive(:is_a?).with(Net::HTTPSuccess).and_return(false)
      allow(Net::HTTP).to receive(:get_response).and_return(bad)
      expect { fetcher.fetch(from_date: Date.new(2026, 10, 1), to_date: Date.new(2026, 10, 31)) }.to raise_error(Tickrake::Error, /403/)
    end
  end
end
