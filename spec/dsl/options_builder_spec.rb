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

  describe "#batch_size" do
    it "defaults batch_size to 1" do
      expect(builder.build![:batch_size]).to eq(1)
    end

    it "sets a custom positive integer batch_size" do
      builder.batch_size(5)
      expect(builder.build![:batch_size]).to eq(5)
    end

    it "raises ArgumentError when batch_size is less than 1" do
      expect { builder.batch_size(0) }.to raise_error(ArgumentError, /batch_size must be a positive integer/)
      expect { builder.batch_size(-2) }.to raise_error(ArgumentError, /batch_size must be a positive integer/)
    end
  end

  describe "#build!" do
    it "returns empty dte_buckets and default batch_size of 1 by default" do
      expect(builder.build!).to eq({ dte_buckets: [], batch_size: 1 })
    end
  end
end
