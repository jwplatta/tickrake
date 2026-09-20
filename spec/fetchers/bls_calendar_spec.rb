# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::Fetchers::BlsCalendar do
  subject(:calendar) { described_class.new }

  let(:ics_body) { <<~ICS }
    BEGIN:VCALENDAR
    BEGIN:VEVENT
    DTSTART;TZID=US-Eastern:20261014T083000
    DTEND;TZID=US-Eastern:20261014T083000
    SUMMARY:Consumer Price Index
    END:VEVENT
    BEGIN:VEVENT
    DTSTART;TZID=US-Eastern:20261022T083000
    DTEND;TZID=US-Eastern:20261022T083000
    SUMMARY:Employment Situation
    END:VEVENT
    BEGIN:VEVENT
    DTSTART;TZID=US-Eastern:20270115T083000
    DTEND;TZID=US-Eastern:20270115T083000
    SUMMARY:Consumer Price Index
    END:VEVENT
    END:VCALENDAR
  ICS

  before do
    response = instance_double(Net::HTTPSuccess, body: ics_body, is_a?: true)
    allow(response).to receive(:is_a?).with(Net::HTTPSuccess).and_return(true)
    allow(Net::HTTP).to receive(:start).and_yield(
      instance_double(Net::HTTP, request: response)
    )
  end

  describe "#fetch" do
    let(:from_date) { Date.new(2026, 10, 1) }
    let(:to_date)   { Date.new(2026, 10, 31) }

    it "returns events within the window" do
      rows = calendar.fetch(from_date: from_date, to_date: to_date)
      expect(rows.length).to eq(2)
    end

    it "excludes events outside the window" do
      rows = calendar.fetch(from_date: from_date, to_date: to_date)
      expect(rows.map { |r| r[:event_name] }).to contain_exactly("Consumer Price Index", "Employment Situation")
    end

    it "converts US-Eastern datetimes to UTC with Z suffix" do
      rows = calendar.fetch(from_date: from_date, to_date: to_date)
      cpi = rows.find { |r| r[:event_name] == "Consumer Price Index" }
      # 8:30 AM ET on 2026-10-14 = 12:30 UTC (EDT = UTC-4)
      expect(cpi[:event_datetime]).to eq("2026-10-14T12:30:00Z")
    end

    it "tags rows with source=bls and category=economic" do
      rows = calendar.fetch(from_date: from_date, to_date: to_date)
      expect(rows).to all(include(source: "bls", category: "economic"))
    end

    it "sets fetched_at as a UTC ISO8601 string" do
      rows = calendar.fetch(from_date: from_date, to_date: to_date)
      expect(rows.first[:fetched_at]).to match(/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\z/)
    end
  end
end
