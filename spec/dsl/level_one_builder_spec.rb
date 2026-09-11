# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::DSL::LevelOneBuilder do
  def build(inline_symbols: ["/ES"], job_name: "test", &block)
    b = described_class.new
    b.instance_eval(&block)
    b.build!(job_name: job_name, inline_symbols: inline_symbols)
  end

  describe "futures" do
    subject(:config) do
      build do
        services [:level_one_futures]
        rotation_interval 300
      end
    end

    it "stores services as DSL symbols for stream.on" do
      expect(config.services).to eq([:level_one_futures])
    end

    it "sets rotation_interval_seconds" do
      expect(config.rotation_interval_seconds).to eq(300)
    end
  end

  describe "multiple services" do
    it "accepts equities and options together" do
      config = build(inline_symbols: ["SPY"]) do
        services [:level_one_equities, :level_one_options]
      end
      expect(config.services).to eq(%i[level_one_equities level_one_options])
    end
  end

  describe "defaults" do
    it "defaults rotation_interval_seconds to 900" do
      config = build { services [:level_one_futures] }
      expect(config.rotation_interval_seconds).to eq(900)
    end
  end

  describe "validations" do
    it "raises with no services" do
      expect do
        build { rotation_interval 300 }
      end.to raise_error(Tickrake::Error, /requires at least one service/)
    end

    it "raises with no symbols" do
      expect do
        build(inline_symbols: []) { services [:level_one_futures] }
      end.to raise_error(Tickrake::Error, /requires symbols/)
    end

    it "raises with an unknown service" do
      expect do
        build { services [:not_a_real_service] }
      end.to raise_error(Tickrake::Error, /Unknown level_one services/)
    end
  end
end
