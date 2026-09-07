# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::DSL::NumericDuration do
  describe "Integer extensions" do
    describe "#seconds / #second" do
      it "returns a NumericDuration in seconds" do
        expect(5.seconds).to be_a(described_class)
        expect(5.seconds.unit).to eq(:seconds)
        expect(5.second.unit).to eq(:seconds)
      end
    end

    describe "#minutes / #minute" do
      it "returns a NumericDuration in minutes" do
        expect(10.minutes.unit).to eq(:minutes)
        expect(1.minute.unit).to eq(:minutes)
      end
    end

    describe "#hours / #hour" do
      it "returns a NumericDuration in hours" do
        expect(2.hours.unit).to eq(:hours)
        expect(1.hour.unit).to eq(:hours)
      end
    end

    describe "#days / #day" do
      it "returns a NumericDuration in days" do
        expect(90.days.unit).to eq(:days)
        expect(1.day.unit).to eq(:days)
      end
    end

    describe "#weeks / #week" do
      it "returns a NumericDuration in weeks" do
        expect(2.weeks.unit).to eq(:weeks)
        expect(1.week.unit).to eq(:weeks)
      end
    end
  end

  describe "#to_interval_seconds" do
    it "converts seconds" do
      expect(5.seconds.to_interval_seconds).to eq(5)
    end

    it "converts minutes" do
      expect(10.minutes.to_interval_seconds).to eq(600)
      expect(30.minutes.to_interval_seconds).to eq(1800)
    end

    it "converts hours" do
      expect(1.hour.to_interval_seconds).to eq(3600)
      expect(2.hours.to_interval_seconds).to eq(7200)
    end

    it "raises for non-time units" do
      expect { 5.days.to_interval_seconds }.to raise_error(Tickrake::Error)
    end
  end

  describe "#to_lookback_days" do
    it "converts days" do
      expect(90.days.to_lookback_days).to eq(90)
    end

    it "converts weeks" do
      expect(2.weeks.to_lookback_days).to eq(14)
    end

    it "raises for non-day units" do
      expect { 10.minutes.to_lookback_days }.to raise_error(Tickrake::Error)
    end
  end
end
