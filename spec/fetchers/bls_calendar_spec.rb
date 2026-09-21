# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::Fetchers::BlsCalendar do
  subject(:calendar) { described_class.new }

  let(:ics_body) { <<~ICS }
    BEGIN:VCALENDAR
    BEGIN:VEVENT
    DTSTART;TZID=US-Eastern:20261014T083000
    SUMMARY:Consumer Price Index
    END:VEVENT
    BEGIN:VEVENT
    DTSTART;TZID=US-Eastern:20261022T083000
    SUMMARY:Employment Situation
    END:VEVENT
    BEGIN:VEVENT
    DTSTART;TZID=US-Eastern:20270115T083000
    SUMMARY:Consumer Price Index
    END:VEVENT
    END:VCALENDAR
  ICS

  before do
    response = instance_double(Net::HTTPSuccess, body: ics_body)
    allow(response).to receive(:is_a?).with(Net::HTTPSuccess).and_return(true)
    allow(Net::HTTP).to receive(:start).and_yield(
      instance_double(Net::HTTP, request: response)
    )
  end

  describe "#fetch" do
    let(:from_date) { Date.new(2026, 10, 1) }
    let(:to_date)   { Date.new(2026, 10, 31) }

    it "filters to the window and converts US-Eastern to UTC" do
      rows = calendar.fetch(from_date: from_date, to_date: to_date)
      expect(rows.length).to eq(2)
      cpi = rows.find { |r| r[:event_name] == "Consumer Price Index" }
      # 8:30 AM ET on 2026-10-14 = 12:30 UTC (EDT = UTC-4)
      expect(cpi[:event_datetime]).to eq("2026-10-14T12:30:00Z")
    end
  end
end
