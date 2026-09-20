# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::Fetchers::FomcCalendar do
  let(:client) { instance_double(Tickrake::Fetchers::FredClient) }
  subject(:calendar) { described_class.new(client: client) }

  let(:from_date) { Date.new(2026, 9, 1) }
  let(:to_date)   { Date.new(2026, 9, 30) }

  before do
    allow(client).to receive(:get).with("/release/dates", anything).and_return({
      "release_dates" => [
        { "date" => "2026-09-17" },
        { "date" => "2026-10-29" }  # outside window
      ]
    })
    allow(client).to receive(:get).with("/series/observations", anything).and_return({
      "observations" => [{ "date" => "2026-09-17", "value" => "5.25" }]
    })
  end

  describe "#fetch" do
    it "filters to window, converts 2pm ET to UTC, and attaches rate value" do
      rows = calendar.fetch(from_date: from_date, to_date: to_date)
      expect(rows.length).to eq(1)
      # 2:00 PM ET on 2026-09-17 = 18:00 UTC (EDT = UTC-4)
      expect(rows.first[:event_datetime]).to eq("2026-09-17T18:00:00Z")
      expect(rows.first[:actual]).to eq(5.25)
    end

    it "returns empty array and warns when FRED call fails" do
      logger = instance_double(Logger, warn: nil)
      allow(client).to receive(:get).and_raise(Tickrake::Error, "API down")
      rows = described_class.new(client: client, logger: logger).fetch(from_date: from_date, to_date: to_date)
      expect(rows).to eq([])
      expect(logger).to have_received(:warn).at_least(:once)
    end
  end
end
