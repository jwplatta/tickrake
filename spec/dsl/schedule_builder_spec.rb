# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::DSL::ScheduleBuilder do
  subject(:builder) { described_class.new }

  describe "#every" do
    it "sets interval_seconds from a NumericDuration" do
      builder.every(10.minutes)
      expect(builder.build![:interval_seconds]).to eq(600)
    end

    it "accepts seconds" do
      builder.every(5.seconds)
      expect(builder.build![:interval_seconds]).to eq(5)
    end
  end

  describe "#at" do
    it "parses a clock string into [hour, minute]" do
      builder.at("16:30")
      expect(builder.build![:run_at]).to eq([16, 30])
    end

    it "parses single-digit hours" do
      builder.at("8:30")
      expect(builder.build![:run_at]).to eq([8, 30])
    end

    it "raises on invalid clock string" do
      expect { builder.at("25:00") }.not_to raise_error
      builder2 = described_class.new
      expect { builder2.at("not-a-time") }.to raise_error(Tickrake::Error)
    end
  end

  describe "#weekdays" do
    context "with no arguments" do
      it "sets days to Mon-Fri" do
        builder.weekdays
        result = builder.build!
        expect(result[:days]).to eq(%w[mon tue wed thu fri])
        expect(result[:windows]).to be_empty
      end
    end

    context "with from: and to: kwargs" do
      it "adds a SchedulerWindow for Mon-Fri" do
        builder.weekdays(from: "08:30", to: "15:05")
        result = builder.build!
        expect(result[:windows].length).to eq(1)
        window = result[:windows].first
        expect(window).to be_a(Tickrake::SchedulerWindow)
        expect(window.days).to eq(%w[mon tue wed thu fri])
        expect(window.start_time).to eq([8, 30])
        expect(window.end_time).to eq([15, 5])
        expect(result[:days]).to be_empty
      end
    end
  end

  describe "#weekends" do
    context "with no arguments" do
      it "sets days to Sat-Sun" do
        builder.weekends
        expect(builder.build![:days]).to eq(%w[sat sun])
      end
    end

    context "with from: and to: kwargs" do
      it "adds a SchedulerWindow for Sat-Sun" do
        builder.weekends(from: "09:00", to: "14:00")
        window = builder.build![:windows].first
        expect(window.days).to eq(%w[sat sun])
        expect(window.start_time).to eq([9, 0])
      end
    end
  end

  describe "#every_day" do
    context "with no arguments" do
      it "sets days to all 7 days" do
        builder.every_day
        expect(builder.build![:days]).to eq(%w[mon tue wed thu fri sat sun])
      end
    end

    context "with from: and to: kwargs" do
      it "adds a SchedulerWindow for all 7 days" do
        builder.every_day(from: "08:30", to: "16:00")
        window = builder.build![:windows].first
        expect(window.days).to eq(%w[mon tue wed thu fri sat sun])
        expect(window.start_time).to eq([8, 30])
        expect(window.end_time).to eq([16, 0])
      end
    end
  end

  describe "#days" do
    it "adds a SchedulerWindow with custom days" do
      builder.days(%w[mon wed fri], from: "09:00", to: "15:00")
      result = builder.build!
      expect(result[:windows].length).to eq(1)
      window = result[:windows].first
      expect(window.days).to eq(%w[mon wed fri])
      expect(window.start_time).to eq([9, 0])
      expect(window.end_time).to eq([15, 0])
    end
  end

  describe "#build!" do
    it "returns all defaults when nothing is set" do
      result = builder.build!
      expect(result[:interval_seconds]).to be_nil
      expect(result[:windows]).to eq([])
      expect(result[:run_at]).to be_nil
      expect(result[:days]).to eq([])
    end

    it "accumulates multiple windows" do
      builder.weekdays(from: "08:30", to: "12:00")
      builder.weekdays(from: "13:00", to: "15:05")
      expect(builder.build![:windows].length).to eq(2)
    end
  end
end
