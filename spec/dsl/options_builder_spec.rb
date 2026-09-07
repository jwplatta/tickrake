# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::DSL::OptionsBuilder do
  subject(:builder) { described_class.new }

  describe "#dte" do
    it "accepts a Range and expands to an array" do
      builder.dte(0..30)
      expect(builder.build![:dte_buckets]).to eq((0..30).to_a)
    end

    it "accepts an explicit Array" do
      builder.dte([0, 5, 10])
      expect(builder.build![:dte_buckets]).to eq([0, 5, 10])
    end

    it "accepts variadic integers" do
      builder.dte(0, 1, 5, 10)
      expect(builder.build![:dte_buckets]).to eq([0, 1, 5, 10])
    end

    it "accepts a single integer" do
      builder.dte(0)
      expect(builder.build![:dte_buckets]).to eq([0])
    end
  end

  describe "#build!" do
    it "returns empty dte_buckets by default" do
      expect(builder.build![:dte_buckets]).to eq([])
    end
  end
end
