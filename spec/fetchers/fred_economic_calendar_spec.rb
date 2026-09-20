# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::Fetchers::FredEconomicCalendar do
  let(:client) { instance_double(Tickrake::Fetchers::FredClient) }
  subject(:calendar) { described_class.new(client: client) }

  let(:from_date) { Date.new(2026, 9, 1) }
  let(:to_date)   { Date.new(2026, 9, 30) }

  before do
    # Default: all release/dates calls return empty
    allow(client).to receive(:get).with("/release/dates", anything).and_return({ "release_dates" => [] })
    allow(client).to receive(:get).with("/series/observations", anything).and_return({ "observations" => [] })
  end

  describe "#fetch" do
    it "returns rows for a release date in window" do
      allow(client).to receive(:get).with("/release/dates", hash_including(release_id: 53)).and_return({
        "release_dates" => [{ "date" => "2026-09-25" }]
      })

      rows = calendar.fetch(from_date: from_date, to_date: to_date)
      gdp_row = rows.find { |r| r[:event_name] == "GDP" }
      expect(gdp_row).not_to be_nil
    end

    it "excludes release dates outside the window" do
      allow(client).to receive(:get).with("/release/dates", hash_including(release_id: 53)).and_return({
        "release_dates" => [
          { "date" => "2026-08-01" },  # before window
          { "date" => "2026-09-25" }   # in window
        ]
      })

      rows = calendar.fetch(from_date: from_date, to_date: to_date)
      expect(rows.map { |r| r[:event_datetime] }).to all(start_with("2026-09-"))
    end

    it "converts release time to UTC with Z suffix" do
      allow(client).to receive(:get).with("/release/dates", hash_including(release_id: 53)).and_return({
        "release_dates" => [{ "date" => "2026-09-25" }]
      })

      rows = calendar.fetch(from_date: from_date, to_date: to_date)
      gdp = rows.find { |r| r[:event_name] == "GDP" }
      # GDP releases at 8:30 AM ET; 2026-09-25 is EDT (UTC-4) → 12:30 UTC
      expect(gdp[:event_datetime]).to eq("2026-09-25T12:30:00Z")
    end

    it "fetches actuals for past series dates" do
      past_date = (Date.today - 5).iso8601
      allow(client).to receive(:get).with("/release/dates", hash_including(release_id: 53)).and_return({
        "release_dates" => [{ "date" => past_date }]
      })
      allow(client).to receive(:get).with("/series/observations", hash_including(series_id: "GDP")).and_return({
        "observations" => [{ "date" => past_date, "value" => "23000.5" }]
      })

      rows = calendar.fetch(from_date: Date.today - 30, to_date: Date.today + 90)
      gdp = rows.find { |r| r[:event_name] == "GDP" }
      expect(gdp[:actual]).to eq(23_000.5)
    end

    it "sets source=fred, category=economic" do
      allow(client).to receive(:get).with("/release/dates", hash_including(release_id: 53)).and_return({
        "release_dates" => [{ "date" => "2026-09-25" }]
      })

      rows = calendar.fetch(from_date: from_date, to_date: to_date)
      expect(rows.find { |r| r[:event_name] == "GDP" }).to include(source: "fred", category: "economic")
    end

    it "skips a release and continues when one fails" do
      logger = instance_double(Logger, warn: nil)
      cal = described_class.new(client: client, logger: logger)
      allow(client).to receive(:get).with("/release/dates", hash_including(release_id: 53)).and_raise(Tickrake::Error, "timeout")

      expect { cal.fetch(from_date: from_date, to_date: to_date) }.not_to raise_error
      expect(logger).to have_received(:warn).at_least(:once)
    end
  end
end
