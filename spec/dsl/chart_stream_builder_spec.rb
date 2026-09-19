# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::DSL::ChartStreamBuilder do
  def build(inline_symbols: ["SPY"], job_name: "test", &block)
    b = described_class.new
    b.instance_eval(&block)
    b.build!(job_name: job_name, inline_symbols: inline_symbols)
  end

  describe "equity charts" do
    subject(:config) do
      build do
        services [:chart_equity]
        flush_interval 30
      end
    end

    it "stores services as DSL symbols" do
      expect(config.services).to eq([:chart_equity])
    end

    it "sets flush_interval_seconds" do
      expect(config.flush_interval_seconds).to eq(30)
    end
  end

  describe "multiple services" do
    it "accepts equity and futures together" do
      config = build do
        services [:chart_equity, :chart_futures]
      end
      expect(config.services).to eq(%i[chart_equity chart_futures])
    end
  end

  describe "defaults" do
    it "defaults flush_interval_seconds to 60" do
      config = build { services [:chart_equity] }
      expect(config.flush_interval_seconds).to eq(60)
    end
  end

  describe "validations" do
    it "raises with no services" do
      expect do
        build { flush_interval 30 }
      end.to raise_error(Tickrake::Error, /requires at least one service/)
    end

    it "raises with no symbols" do
      expect do
        build(inline_symbols: []) { services [:chart_equity] }
      end.to raise_error(Tickrake::Error, /requires symbols/)
    end

    it "raises with an unknown service" do
      expect do
        build { services [:not_a_real_service] }
      end.to raise_error(Tickrake::Error, /Unknown chart_stream services/)
    end
  end
end
