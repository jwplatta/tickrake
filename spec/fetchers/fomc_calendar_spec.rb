# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::Fetchers::FomcCalendar do
  let(:client) { instance_double(Tickrake::Fetchers::FredClient) }
  subject(:calendar) { described_class.new(client: client) }

  let(:from_date) { Date.new(2026, 9, 1) }
  let(:to_date)   { Date.new(2026, 9, 30) }

  before do
    allow(client).to receive(:get).with("/release/dates", hash_including(release_id: 21)).and_return({
      "release_dates" => [
        { "date" => "2026-09-17" },
        { "date" => "2026-10-29" }  # outside window
      ]
    })
    allow(client).to receive(:get).with("/series/observations", hash_including(series_id: "DFEDTARU")).and_return({
      "observations" => [
        { "date" => "2026-09-17", "value" => "5.25" },
        { "date" => "2026-09-16", "value" => "5.50" }
      ]
    })
  end

  describe "#fetch" do
    it "returns only meeting dates within the window" do
      rows = calendar.fetch(from_date: from_date, to_date: to_date)
      expect(rows.length).to eq(1)
    end

    it "sets event_name to FOMC Rate Decision" do
      rows = calendar.fetch(from_date: from_date, to_date: to_date)
      expect(rows.first[:event_name]).to eq("FOMC Rate Decision")
    end

    it "sets the rate decision time to 2:00 PM ET in UTC" do
      rows = calendar.fetch(from_date: from_date, to_date: to_date)
      # 2:00 PM ET on 2026-09-17 = 18:00 UTC (EDT = UTC-4)
      expect(rows.first[:event_datetime]).to eq("2026-09-17T18:00:00Z")
    end

    it "attaches the actual rate value" do
      rows = calendar.fetch(from_date: from_date, to_date: to_date)
      expect(rows.first[:actual]).to eq(5.25)
    end

    it "sets category=fomc, source=fred, unit=percent" do
      rows = calendar.fetch(from_date: from_date, to_date: to_date)
      expect(rows.first).to include(category: "fomc", source: "fred", unit: "percent")
    end

    it "returns empty array when no meeting dates in window" do
      allow(client).to receive(:get).with("/release/dates", anything).and_return({ "release_dates" => [] })
      expect(calendar.fetch(from_date: from_date, to_date: to_date)).to eq([])
    end

    it "returns empty array and logs when FRED call fails" do
      logger = instance_double(Logger, warn: nil)
      cal = described_class.new(client: client, logger: logger)
      allow(client).to receive(:get).and_raise(Tickrake::Error, "API down")
      rows = cal.fetch(from_date: from_date, to_date: to_date)
      expect(rows).to eq([])
      expect(logger).to have_received(:warn).at_least(:once)
    end
  end
end
