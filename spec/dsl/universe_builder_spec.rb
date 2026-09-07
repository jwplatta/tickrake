# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::DSL::UniverseBuilder do
  subject(:builder) { described_class.new }

  describe "#ticker" do
    it "adds a UniverseEntry with just a symbol" do
      builder.ticker("SPY")
      expect(builder.entries.length).to eq(1)
      entry = builder.entries.first
      expect(entry).to be_a(Tickrake::UniverseEntry)
      expect(entry.symbol).to eq("SPY")
      expect(entry.option_root).to be_nil
      expect(entry.option_roots).to eq([])
    end

    it "adds a UniverseEntry with a single option_root" do
      builder.ticker("$SPX", option_root: "SPXW")
      entry = builder.entries.first
      expect(entry.symbol).to eq("$SPX")
      expect(entry.option_root).to eq("SPXW")
    end

    it "adds a UniverseEntry with multiple option_roots" do
      builder.ticker("$SPX", option_roots: %w[SPXW SPXM])
      entry = builder.entries.first
      expect(entry.option_roots).to eq(%w[SPXW SPXM])
    end

    it "accumulates multiple entries in order" do
      builder.ticker("$SPX", option_root: "SPXW")
      builder.ticker("SPY")
      builder.ticker("QQQ")
      expect(builder.entries.map(&:symbol)).to eq(%w[$SPX SPY QQQ])
    end
  end
end
