# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::DSL::OrderBookBuilder do
  def build(inline_symbols: ["SPY"], job_name: "test", &block)
    b = described_class.new
    b.instance_eval(&block)
    b.build!(job_name: job_name, inline_symbols: inline_symbols)
  end

  describe "equity book" do
    subject(:config) do
      build do
        services [:nyse_book, :nasdaq_book]
        rotation_interval 600
      end
    end

    it "normalizes service symbols to Schwab constants" do
      expect(config.services).to eq(%w[NYSE_BOOK NASDAQ_BOOK])
    end

    it "sets rotation_interval_seconds" do
      expect(config.rotation_interval_seconds).to eq(600)
    end

    it "returns false for options_book?" do
      expect(config.options_book?).to be(false)
    end
  end

  describe "options book" do
    subject(:config) do
      build(inline_symbols: []) do
        services [:options_book]
        rotation_interval 600
        contracts do
          underlying "SPXW"
          atm_strikes 5
          expirations front: 2
          re_resolve_interval 30
        end
      end
    end

    it "returns true for options_book?" do
      expect(config.options_book?).to be(true)
    end

    it "builds a contracts config" do
      expect(config.contracts).to be_a(Tickrake::OrderBookContractsConfig)
      expect(config.contracts.underlying).to eq("SPXW")
      expect(config.contracts.atm_strikes).to eq(5)
      expect(config.contracts.front_n).to eq(2)
    end

    it "converts re_resolve_interval to seconds" do
      expect(config.re_resolve_interval_seconds).to eq(1800)
    end
  end

  describe "defaults" do
    it "defaults rotation_interval_seconds to 900" do
      config = build do
        services [:nyse_book]
      end
      expect(config.rotation_interval_seconds).to eq(900)
    end
  end

  describe "validations" do
    it "raises when mixing equity and options_book" do
      expect do
        build { services [:nyse_book, :options_book] }
      end.to raise_error(Tickrake::Error, /cannot mix/)
    end

    it "raises when options_book has no contracts block" do
      expect do
        build(inline_symbols: []) { services [:options_book] }
      end.to raise_error(Tickrake::Error, /requires a contracts block/)
    end

    it "raises when equity book has no symbols" do
      expect do
        build(inline_symbols: []) { services [:nyse_book] }
      end.to raise_error(Tickrake::Error, /requires top-level symbols/)
    end

    it "raises with an unknown service" do
      expect do
        build { services [:level_one_futures] }
      end.to raise_error(Tickrake::Error, /Unknown order_book services/)
    end

    it "raises when no services are given" do
      expect do
        build { rotation_interval 600 }
      end.to raise_error(Tickrake::Error, /requires at least one service/)
    end
  end
end
