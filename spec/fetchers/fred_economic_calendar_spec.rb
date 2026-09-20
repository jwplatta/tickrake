# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::Fetchers::FredEconomicCalendar do
  let(:client) { instance_double(Tickrake::Fetchers::FredClient) }
  subject(:calendar) { described_class.new(client: client) }

  let(:from_date) { Date.new(2026, 9, 1) }
  let(:to_date)   { Date.new(2026, 9, 30) }

  before do
    allow(client).to receive(:get).with("/release/dates", anything).and_return({ "release_dates" => [] })
    allow(client).to receive(:get).with("/series/observations", anything).and_return({ "observations" => [] })
  end

  describe "#fetch" do
    it "filters to window and converts release time to UTC" do
      allow(client).to receive(:get).with("/release/dates", hash_including(release_id: 53)).and_return({
        "release_dates" => [{ "date" => "2026-08-01" }, { "date" => "2026-09-25" }]
      })
      rows = calendar.fetch(from_date: from_date, to_date: to_date)
      gdp = rows.find { |r| r[:event_name] == "GDP" }
      expect(gdp).not_to be_nil
      # GDP at 8:30 AM ET; 2026-09-25 is EDT (UTC-4) → 12:30 UTC
      expect(gdp[:event_datetime]).to eq("2026-09-25T12:30:00Z")
    end

    it "skips a failing release and continues with others" do
      logger = instance_double(Logger, warn: nil)
      allow(client).to receive(:get).with("/release/dates", hash_including(release_id: 53)).and_raise(Tickrake::Error, "timeout")
      expect { described_class.new(client: client, logger: logger).fetch(from_date: from_date, to_date: to_date) }.not_to raise_error
      expect(logger).to have_received(:warn).at_least(:once)
    end
  end
end
