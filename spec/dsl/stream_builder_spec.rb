# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::DSL::StreamBuilder do
  let(:builder) { described_class.new }

  describe "#build!" do
    it "raises when no subscriptions are defined" do
      expect {
        builder.build!(job_name: "empty_stream")
      }.to raise_error(Tickrake::Error, /requires at least one subscription block/)
    end

    it "builds a StreamConfig with level_one, order_book, and chart_stream subscriptions" do
      builder.stale_timeout(90)

      builder.level_one "equities" do
        symbols "SPY", "QQQ"
        services [:level_one_equities]
        rotation_interval 300
        schedule do
          weekdays from: "08:30", to: "15:00"
        end
      end

      builder.order_book "equity_books" do
        symbols "SPY"
        services [:nyse_book]
        rotation_interval 300
      end

      builder.chart_stream "futures_candles" do
        symbols "/ES"
        services [:chart_futures]
        flush_interval 60
        schedule do
          days %w[sun mon tue wed thu], from: "17:00", to: "23:59"
        end
      end

      config = builder.build!(job_name: "all_streams")
      expect(config.stale_timeout_seconds).to eq(90)
      expect(config.subscriptions.size).to eq(3)

      sub1 = config.subscriptions[0]
      expect(sub1.name).to eq("equities")
      expect(sub1.kind).to eq(:level_one)
      expect(sub1.symbols).to eq(%w[SPY QQQ])
      expect(sub1.settings.services).to eq([:level_one_equities])
      expect(sub1.windows.size).to eq(1)

      sub2 = config.subscriptions[1]
      expect(sub2.name).to eq("equity_books")
      expect(sub2.kind).to eq(:order_book)
      expect(sub2.symbols).to eq(["SPY"])
      expect(sub2.settings.services).to eq(["NYSE_BOOK"])
      expect(sub2.windows).to be_empty

      sub3 = config.subscriptions[2]
      expect(sub3.name).to eq("futures_candles")
      expect(sub3.kind).to eq(:chart_stream)
      expect(sub3.symbols).to eq(["/ES"])
      expect(sub3.settings.services).to eq([:chart_futures])
      expect(sub3.windows.size).to eq(1)
    end

    it "raises when a subscription is missing symbols" do
      builder.level_one "no_symbols" do
        services [:level_one_equities]
      end

      expect {
        builder.build!(job_name: "bad_stream")
      }.to raise_error(Tickrake::Error, /requires symbols/)
    end

    it "raises when a subscription has unknown services" do
      builder.level_one "bad_service" do
        symbols "SPY"
        services [:invalid_service]
      end

      expect {
        builder.build!(job_name: "bad_stream")
      }.to raise_error(Tickrake::Error, /Unknown level_one services/)
    end
  end
end
