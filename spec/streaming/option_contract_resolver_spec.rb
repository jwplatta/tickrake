# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::Streaming::OptionContractResolver do
  let(:client) { instance_double("SchwabRb::Client") }

  def make_option(symbol:, strike:, exp_date:, put_call:, root: "SPXW")
    double(
      "Option",
      symbol: symbol,
      strike: strike,
      expiration_date: exp_date,
      put_call: put_call,
      option_root: root
    )
  end

  describe "#resolve" do
    let(:exp1) { Date.new(2026, 9, 21) }
    let(:exp2) { Date.new(2026, 9, 22) }

    let(:call_opts) do
      [
        make_option(symbol: "SPXW  260921C05940000", strike: 5940.0, exp_date: exp1, put_call: "CALL", root: "SPXW"),
        make_option(symbol: "SPXW  260921C05950000", strike: 5950.0, exp_date: exp1, put_call: "CALL", root: "SPXW"),
        make_option(symbol: "SPXW  260921C06000000", strike: 6000.0, exp_date: exp1, put_call: "CALL", root: "SPXW"),
        make_option(symbol: "SPXW  260921C06050000", strike: 6050.0, exp_date: exp1, put_call: "CALL", root: "SPXW"),
        make_option(symbol: "SPXW  260921C06060000", strike: 6060.0, exp_date: exp1, put_call: "CALL", root: "SPXW"),
        make_option(symbol: "SPX   260921C06000000", strike: 6000.0, exp_date: exp1, put_call: "CALL", root: "SPX"),
        make_option(symbol: "SPXW  260922C06000000", strike: 6000.0, exp_date: exp2, put_call: "CALL", root: "SPXW")
      ]
    end

    let(:put_opts) do
      [
        make_option(symbol: "SPXW  260921P05940000", strike: 5940.0, exp_date: exp1, put_call: "PUT", root: "SPXW"),
        make_option(symbol: "SPXW  260921P05950000", strike: 5950.0, exp_date: exp1, put_call: "PUT", root: "SPXW"),
        make_option(symbol: "SPXW  260921P06000000", strike: 6000.0, exp_date: exp1, put_call: "PUT", root: "SPXW"),
        make_option(symbol: "SPXW  260921P06050000", strike: 6050.0, exp_date: exp1, put_call: "PUT", root: "SPXW"),
        make_option(symbol: "SPXW  260921P06060000", strike: 6060.0, exp_date: exp1, put_call: "PUT", root: "SPXW")
      ]
    end

    let(:chain) do
      double(
        "OptionChain",
        underlying_price: 6000.0,
        call_opts: call_opts,
        put_opts: put_opts
      )
    end

    it "filters by option_root, front_n expirations, and atm_points around underlying_price" do
      contracts_config = Tickrake::OptionContractsConfig.new(
        underlying: "SPX",
        option_root: "SPXW",
        atm_points: 50.0,
        front_n: 1
      )

      expect(client).to receive(:get_option_chain).with("$SPX", include_underlying_quote: true).and_return(chain)

      resolver = described_class.new(client, contracts_config)
      symbols = resolver.resolve

      expect(symbols).to eq([
        "SPXW  260921C05950000",
        "SPXW  260921C06000000",
        "SPXW  260921C06050000",
        "SPXW  260921P05950000",
        "SPXW  260921P06000000",
        "SPXW  260921P06050000"
      ])
    end

    it "supports atm_strikes when atm_points is not specified" do
      contracts_config = Tickrake::OptionContractsConfig.new(
        underlying: "SPX",
        option_root: "SPXW",
        atm_strikes: 1,
        front_n: 1
      )

      expect(client).to receive(:get_option_chain).with("$SPX", include_underlying_quote: true, strike_count: 2).and_return(chain)

      resolver = described_class.new(client, contracts_config)
      symbols = resolver.resolve

      # 1 strike <= 6000.0 (6000.0) and 1 strike > 6000.0 (6050.0)
      expect(symbols).to eq([
        "SPXW  260921C06000000",
        "SPXW  260921C06050000",
        "SPXW  260921P06000000",
        "SPXW  260921P06050000"
      ])
    end
  end
end
