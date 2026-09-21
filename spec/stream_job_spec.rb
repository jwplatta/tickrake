# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::StreamJob do
  let(:pending_events_dir) { Dir.mktmpdir("events") }
  let(:candles_dir) { Dir.mktmpdir("candles") }
  let(:logger) { instance_double(Logger, info: nil, warn: nil, error: nil) }

  let(:config) do
    instance_double(
      Tickrake::Config,
      pending_events_dir: pending_events_dir,
      candles_dir: candles_dir
    ).tap do |c|
      allow(c).to receive(:option_snapshot_filename_timezone).and_return("UTC")
    end
  end

  let(:runtime) do
    instance_double(Tickrake::Runtime, config: config, logger: logger)
  end

  let(:equities_sub) do
    Tickrake::StreamSubscription.new(
      name: "equity_l1",
      kind: :level_one,
      symbols: %w[SPY QQQ],
      settings: Tickrake::LevelOneConfig.new(services: [:level_one_equities], rotation_interval_seconds: 300),
      windows: [
        Tickrake::SchedulerWindow.new(
          days: %w[mon tue wed thu fri],
          start_time: [8, 30],
          end_time: [15, 0]
        )
      ]
    )
  end

  let(:futures_sub) do
    Tickrake::StreamSubscription.new(
      name: "futures_l1",
      kind: :level_one,
      symbols: ["/ES"],
      settings: Tickrake::LevelOneConfig.new(services: [:level_one_futures], rotation_interval_seconds: 300),
      windows: [] # Always in window
    )
  end

  let(:stream_config) do
    Tickrake::StreamConfig.new(
      subscriptions: [futures_sub, equities_sub],
      stale_timeout_seconds: 60
    )
  end

  let(:scheduled_job) do
    instance_double(
      Tickrake::ScheduledJobConfig,
      name: "market_streams",
      provider: "schwab",
      settings: stream_config,
      universe: ["/ES", "SPY", "QQQ"]
    )
  end

  subject(:job) { described_class.new(runtime, scheduled_job: scheduled_job) }

  after do
    FileUtils.rm_rf(pending_events_dir)
    FileUtils.rm_rf(candles_dir)
  end

  class FakeClient
    attr_reader :added_subscriptions, :unsubbed_subscriptions, :callbacks, :started, :stopped

    def initialize
      @added_subscriptions = []
      @unsubbed_subscriptions = []
      @callbacks = {}
      @started = false
      @stopped = false
    end

    def add(service_sym, symbols:, fields:, &block)
      @added_subscriptions << { service: service_sym, symbols: symbols, fields: fields }
      @callbacks[service_sym] = block
      self
    end
    alias on add

    def unsub(service_sym, symbols:)
      @unsubbed_subscriptions << { service: service_sym, symbols: symbols }
      @callbacks.delete(service_sym)
      self
    end

    def start_async
      @started = true
      self
    end

    def stop
      @stopped = true
    end

    def fire(service_sym, event)
      @callbacks[service_sym]&.call(event)
    end
  end

  let(:fake_stream) { FakeClient.new }

  before do
    allow(Tickrake::ClientFactory).to receive(:new).and_return(double(build: nil))
    allow(SchwabRb::Stream::Client).to receive(:new).and_return(fake_stream)
  end

  describe "#sync_subscriptions" do
    before do
      job.instance_variable_set(:@stream, fake_stream)
    end

    it "activates subscriptions that are currently in window" do
      # Sunday 18:00 -> futures active, equities inactive
      sunday_evening = Time.new(2026, 9, 20, 18, 0, 0, "-05:00")
      job.sync_subscriptions(sunday_evening)

      expect(fake_stream.added_subscriptions.size).to eq(1)
      expect(fake_stream.added_subscriptions.first[:service]).to eq(:level_one_futures)
      expect(fake_stream.added_subscriptions.first[:symbols]).to eq(["/ES"])
      expect(fake_stream.unsubbed_subscriptions).to be_empty
    end

    it "dynamically adds in-window subscriptions and removes out-of-window subscriptions" do
      # 1. Start on Sunday evening -> only futures active
      sunday_evening = Time.new(2026, 9, 20, 18, 0, 0, "-05:00")
      job.sync_subscriptions(sunday_evening)
      expect(fake_stream.added_subscriptions.map { |s| s[:service] }).to eq([:level_one_futures])

      # 2. Advance to Monday 09:30 CDT -> equities enter window
      monday_morning = Time.new(2026, 9, 21, 9, 30, 0, "-05:00")
      job.sync_subscriptions(monday_morning)

      expect(fake_stream.added_subscriptions.map { |s| s[:service] }).to contain_exactly(:level_one_futures, :level_one_equities)
      expect(fake_stream.unsubbed_subscriptions).to be_empty

      # 3. Advance to Monday 16:00 CDT -> equities leave window
      monday_evening = Time.new(2026, 9, 21, 16, 0, 0, "-05:00")
      job.sync_subscriptions(monday_evening)

      expect(fake_stream.unsubbed_subscriptions.size).to eq(1)
      expect(fake_stream.unsubbed_subscriptions.first[:service]).to eq(:level_one_equities)
      expect(fake_stream.unsubbed_subscriptions.first[:symbols]).to eq(%w[SPY QQQ])
    end
  end

  describe "event routing and file writing" do
    before do
      job.instance_variable_set(:@stream, fake_stream)
      t = Time.new(2026, 9, 21, 10, 0, 0, "-05:00")
      job.sync_subscriptions(t)
    end

    it "writes received Level 1 events to dedicated EventsWriter" do
      event = {
        "content" => [{
          "key" => "/ES",
          "1"   => 4500.0,
          "2"   => 4501.0,
          "3"   => 4500.5,
          "10"  => 1_695_000_000_000,
          "11"  => 1_695_000_000_000
        }]
      }

      fake_stream.fire(:level_one_futures, event)
      job.close

      # Check generated file in pending_events_dir
      files = Dir.glob(File.join(pending_events_dir, "futures_l1_*.ndjson"))
      expect(files.size).to eq(1)

      lines = File.readlines(files.first)
      expect(lines.size).to eq(1)
      parsed = JSON.parse(lines.first)
      expect(parsed["symbol"]).to eq("/ES")
      expect(parsed["bid"]).to eq(4500.0)
    end
  end
end
