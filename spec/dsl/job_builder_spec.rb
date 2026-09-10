# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::DSL::JobBuilder do
  let(:aapl_entry) do
    Tickrake::UniverseEntry.new(
      symbol: "AAPL", option_root: nil, option_roots: [],
      start_date: nil, need_extended_hours_data: false, need_previous_close: false
    )
  end
  let(:spy_entry) do
    Tickrake::UniverseEntry.new(
      symbol: "SPY", option_root: nil, option_roots: [],
      start_date: nil, need_extended_hours_data: false, need_previous_close: false
    )
  end
  let(:spxw_entry) do
    Tickrake::UniverseEntry.new(
      symbol: "SPX", option_root: "SPXW", option_roots: [],
      start_date: nil, need_extended_hours_data: false, need_previous_close: false
    )
  end

  let(:stock_universe)  { instance_double(Tickrake::UniverseConfig, entries: [aapl_entry, spy_entry]) }
  let(:spx_universe)    { instance_double(Tickrake::UniverseConfig, entries: [spxw_entry]) }

  let(:config) do
    instance_double(Tickrake::Config).tap do |c|
      allow(c).to receive(:universe).with("stock_universe").and_return(stock_universe)
      allow(c).to receive(:universe).with("spx_symbols").and_return(spx_universe)
    end
  end

  def build(name, &block)
    b = described_class.new(name)
    b.instance_eval(&block)
    b.build!(config)
  end

  describe "options job" do
    subject(:job) do
      build("test_options") do
        provider :schwab
        type :options
        universe "stock_universe"
        schedule { every 30.minutes; weekdays from: "08:30", to: "15:05" }
        options  { dte 0..30 }
      end
    end

    it "builds a ScheduledJobConfig" do
      expect(job).to be_a(Tickrake::ScheduledJobConfig)
    end

    it "sets name, type, and provider" do
      expect(job.name).to eq("test_options")
      expect(job.type).to eq("options")
      expect(job.provider).to eq("schwab")
    end

    it "sets interval_seconds from the schedule" do
      expect(job.interval_seconds).to eq(1800)
    end

    it "builds a SchedulerWindow" do
      expect(job.windows.length).to eq(1)
      expect(job.windows.first.start_time).to eq([8, 30])
      expect(job.windows.first.end_time).to eq([15, 5])
    end

    it "sets dte_buckets as integers" do
      expect(job.dte_buckets).to eq((0..30).to_a)
    end

    it "expands universe to OptionSymbol objects" do
      expect(job.universe.length).to eq(2)
      expect(job.universe).to all(be_a(Tickrake::OptionSymbol))
      expect(job.universe.map(&:symbol)).to eq(%w[AAPL SPY])
    end

    it "expands entries with an explicit option_root" do
      job = build("spx_options") do
        provider :schwab
        type :options
        universe "spx_symbols"
        schedule { every 5.seconds; weekdays from: "08:30", to: "15:00" }
        options  { dte 0 }
      end
      expect(job.universe.length).to eq(1)
      sym = job.universe.first
      expect(sym.symbol).to eq("SPX")
      expect(sym.option_root).to eq("SPXW")
    end

    it "infers type from the options block without an explicit type call" do
      job = build("inferred_options") do
        provider :schwab
        universe "stock_universe"
        schedule { every 10.minutes; weekdays from: "08:30", to: "15:05" }
        options  { dte 0..30 }
      end
      expect(job.type).to eq("options")
    end

    it "raises without provider" do
      expect do
        build("bad") do
          type :options
          universe "stock_universe"
          schedule { every 10.minutes; weekdays from: "08:30", to: "15:05" }
          options  { dte 0..30 }
        end
      end.to raise_error(Tickrake::Error, /requires provider/)
    end

    it "raises without a schedule block" do
      expect do
        build("bad") do
          provider :schwab
          type :options
          universe "stock_universe"
          options { dte 0..30 }
        end
      end.to raise_error(Tickrake::Error, /requires a schedule block/)
    end

    it "raises without universe" do
      expect do
        build("bad") do
          provider :schwab
          type :options
          schedule { every 10.minutes; weekdays from: "08:30", to: "15:05" }
          options  { dte 0..30 }
        end
      end.to raise_error(Tickrake::Error, /requires universe/)
    end

    it "raises without an options block" do
      expect do
        build("bad") do
          provider :schwab
          type :options
          universe "stock_universe"
          schedule { every 10.minutes; weekdays from: "08:30", to: "15:05" }
        end
      end.to raise_error(Tickrake::Error, /requires an options block/)
    end

    context "with inline universe block" do
      subject(:job) do
        build("inline_options") do
          provider :schwab
          type :options
          universe do
            ticker "$SPX", option_root: "SPXW"
            ticker "SPY"
            ticker "QQQ"
          end
          schedule { every 5.minutes; weekdays from: "08:30", to: "15:05" }
          options  { dte 0..10 }
        end
      end

      it "does not call config.universe" do
        expect(config).not_to receive(:universe)
        job
      end

      it "builds OptionSymbol objects from inline tickers" do
        syms = job.universe
        expect(syms.map(&:symbol)).to eq(%w[$SPX SPY QQQ])
      end

      it "assigns the option_root from the inline definition" do
        spx_sym = job.universe.find { |s| s.symbol == "$SPX" }
        expect(spx_sym.option_root).to eq("SPXW")
      end

      it "leaves option_root nil for symbols without one" do
        spy_sym = job.universe.find { |s| s.symbol == "SPY" }
        expect(spy_sym.option_root).to be_nil
      end
    end
  end

  describe "candles job" do
    subject(:job) do
      build("test_candles") do
        provider :schwab
        type :candles
        symbols "/ES", "/NQ"
        schedule { at "16:30"; weekdays }
        lookback 90.days
        candles do
          frequencies %w[day 30min 5min 1min]
          start_date "2026-06-01"
        end
      end
    end

    it "builds a ScheduledJobConfig with type candles" do
      expect(job.type).to eq("candles")
    end

    it "sets run_at and days from the schedule" do
      expect(job.run_at).to eq([16, 30])
      expect(job.days).to eq(%w[mon tue wed thu fri])
    end

    it "sets lookback_days" do
      expect(job.lookback_days).to eq(90)
    end

    it "builds CandleSymbol objects from inline symbols" do
      expect(job.universe.length).to eq(2)
      expect(job.universe).to all(be_a(Tickrake::CandleSymbol))
      expect(job.universe.map(&:symbol)).to eq(["/ES", "/NQ"])
    end

    it "embeds frequencies in each CandleSymbol" do
      expect(job.universe.first.frequencies).to eq(%w[day 30min 5min 1min])
    end

    it "embeds start_date in each CandleSymbol" do
      expect(job.universe.first.start_date).to eq(Date.iso8601("2026-06-01"))
    end

    it "raises without symbols or universe" do
      expect do
        build("bad") do
          provider :schwab
          type :candles
          schedule { at "16:30"; weekdays }
          lookback 90.days
          candles { frequencies %w[day]; start_date "2026-06-01" }
        end
      end.to raise_error(Tickrake::Error, /requires symbols or universe/)
    end
  end

  describe "order_book job" do
    subject(:job) do
      build("test_order_book") do
        provider :schwab
        symbols "SPY", "QQQ"
        schedule { weekdays from: "08:30", to: "15:00" }
        order_book do
          services [:nyse_book, :nasdaq_book]
          flush_interval 60
          retention_days 30
        end
      end
    end

    it "infers type as order_book" do
      expect(job.type).to eq("order_book")
    end

    it "stores OrderBookConfig in settings" do
      expect(job.settings).to be_a(Tickrake::OrderBookConfig)
      expect(job.settings.services).to eq(%w[NYSE_BOOK NASDAQ_BOOK])
      expect(job.settings.flush_interval_seconds).to eq(60)
    end

    it "sets universe from inline symbols" do
      expect(job.universe).to eq(%w[SPY QQQ])
    end

    it "raises when mixing equity and options_book services" do
      expect do
        build("bad") do
          provider :schwab
          symbols "SPY"
          schedule { weekdays from: "08:30", to: "15:00" }
          order_book { services [:nyse_book, :options_book] }
        end
      end.to raise_error(Tickrake::Error, /cannot mix/)
    end

    it "raises when options_book is used without a contracts block" do
      expect do
        build("bad") do
          provider :schwab
          schedule { weekdays from: "08:30", to: "15:00" }
          order_book { services [:options_book] }
        end
      end.to raise_error(Tickrake::Error, /requires a contracts block/)
    end
  end

  describe "level_one job" do
    subject(:job) do
      build("test_level_one") do
        provider :schwab
        symbols "/ES"
        schedule { every_day from: "17:00", to: "16:00" }
        level_one do
          services [:level_one_futures]
          flush_interval 300
          retention_days 30
        end
      end
    end

    it "infers type as level_one" do
      expect(job.type).to eq("level_one")
    end

    it "stores LevelOneConfig in settings" do
      expect(job.settings).to be_a(Tickrake::LevelOneConfig)
      expect(job.settings.services).to eq(%i[level_one_futures])
      expect(job.settings.flush_interval_seconds).to eq(300)
    end

    it "sets universe from inline symbols" do
      expect(job.universe).to eq(["/ES"])
    end

    it "raises with an unknown service" do
      expect do
        build("bad") do
          provider :schwab
          symbols "/ES"
          schedule { every_day from: "17:00", to: "16:00" }
          level_one { services [:not_a_real_service] }
        end
      end.to raise_error(Tickrake::Error, /Unknown level_one services/)
    end

    it "raises without symbols" do
      expect do
        build("bad") do
          provider :schwab
          schedule { every_day from: "17:00", to: "16:00" }
          level_one { services [:level_one_futures] }
        end
      end.to raise_error(Tickrake::Error, /requires symbols/)
    end
  end

  describe "type inference" do
    it "raises when no typed block is present" do
      expect do
        build("bad") do
          provider :schwab
          schedule { weekdays from: "08:30", to: "15:00" }
        end
      end.to raise_error(Tickrake::Error, /requires a typed block/)
    end
  end

  describe "maintenance job" do
    subject(:job) do
      build("test_maintenance") do
        provider :schwab
        type :maintenance
        schedule { at "15:30"; weekdays }
        maintenance do
          compact :option_samples, universe: "spx_symbols", delete_sources: true
          archive :option_samples, universe: "spx_symbols",
                  to: :s3_archive, artifacts: %i[csv parquet],
                  retain: { parquet: true }
        end
      end
    end

    it "builds a ScheduledJobConfig with type maintenance" do
      expect(job.type).to eq("maintenance")
    end

    it "sets run_at from the schedule" do
      expect(job.run_at).to eq([15, 30])
    end

    it "builds two MaintenanceStepConfig tasks" do
      expect(job.tasks.length).to eq(2)
      expect(job.tasks.map(&:action)).to eq(%w[compact archive])
    end

    it "sets correct fields on the compact task" do
      step = job.tasks.first
      expect(step.universe).to eq("spx_symbols")
      expect(step.delete_sources).to be(true)
    end

    it "sets correct fields on the archive task" do
      step = job.tasks.last
      expect(step.destination).to eq("s3_archive")
      expect(step.retain_local).to eq("csv" => false, "parquet" => true)
    end

    it "raises without a maintenance block" do
      expect do
        build("bad") do
          provider :schwab
          type :maintenance
          schedule { at "15:30"; weekdays }
        end
      end.to raise_error(Tickrake::Error, /requires a maintenance block/)
    end
  end
end
