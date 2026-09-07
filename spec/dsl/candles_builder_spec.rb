# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::DSL::CandlesBuilder do
  subject(:builder) { described_class.new }

  describe "#frequency" do
    it "sets a single frequency, normalized" do
      builder.frequency("1min")
      expect(builder.build![:frequencies]).to eq(["1min"])
    end

    it "normalizes aliases" do
      builder.frequency("1m")
      expect(builder.build![:frequencies]).to eq(["1min"])
    end

    it "normalizes 'daily' to 'day'" do
      builder.frequency("daily")
      expect(builder.build![:frequencies]).to eq(["day"])
    end
  end

  describe "#frequencies" do
    it "sets multiple frequencies" do
      builder.frequencies(%w[day 30min 5min 1min])
      expect(builder.build![:frequencies]).to eq(%w[day 30min 5min 1min])
    end

    it "normalizes and deduplicates" do
      builder.frequencies("1m", "1min")
      expect(builder.build![:frequencies]).to eq(["1min"])
    end

    it "raises on unsupported frequency" do
      expect { builder.frequencies("2min") }.to raise_error(Tickrake::Error)
    end
  end

  describe "#start_date" do
    it "parses an ISO8601 string into a Date" do
      builder.start_date("2026-06-01")
      expect(builder.build![:start_date]).to eq(Date.iso8601("2026-06-01"))
    end
  end

  describe "#build!" do
    it "returns empty defaults" do
      result = builder.build!
      expect(result[:frequencies]).to eq([])
      expect(result[:start_date]).to be_nil
    end
  end
end
