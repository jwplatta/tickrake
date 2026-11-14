# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::StreamJob do
  let(:data_dir) { Dir.mktmpdir("data") }
  let(:candles_dir) { Dir.mktmpdir("candles") }
  let(:logger) { instance_double(Logger, info: nil, warn: nil, error: nil) }

  let(:config) do
    instance_double(
      Tickrake::Config,
      data_dir: data_dir,
      candles_dir: candles_dir,
      provider_definition: nil
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
      settings: Tickrake::LevelOneConfig.new(services: [:level_one_equities]),
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
      settings: Tickrake::LevelOneConfig.new(services: [:level_one_futures]),
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
    FileUtils.rm_rf(data_dir)
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
      @added_subscriptions << { service: service_sym, symbols: symbols, fields: fields, has_block: !block.nil? }
      @callbacks[service_sym] = block if block
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

    it "resolves dynamic contracts on window entry, adds new ATM contracts mid-session without duplicate callback blocks, and unsubscribes all accumulated contracts at window exit" do
      contracts_cfg = Tickrake::OptionContractsConfig.new(
        underlying: "SPX",
        option_root: "SPXW",
        atm_points: 50.0,
        front_n: 1,
        re_resolve_interval_minutes: 5
      )
      options_sub = Tickrake::StreamSubscription.new(
        name: "spxw_0dte",
        kind: :level_one,
        symbols: [],
        settings: Tickrake::LevelOneConfig.new(
          services: [:level_one_options],
          contracts: contracts_cfg
        ),
        windows: [
          Tickrake::SchedulerWindow.new(
            days: %w[mon tue wed thu fri],
            start_time: [8, 30],
            end_time: [15, 0]
          )
        ]
      )
      dyn_stream_config = Tickrake::StreamConfig.new(
        subscriptions: [options_sub],
        stale_timeout_seconds: 60
      )
      dyn_scheduled_job = instance_double(
        Tickrake::ScheduledJobConfig,
        name: "options_stream",
        provider: "schwab",
        settings: dyn_stream_config,
        universe: []
      )
      dyn_job = described_class.new(runtime, scheduled_job: dyn_scheduled_job)
      dyn_job.instance_variable_set(:@stream, fake_stream)

      resolver1 = instance_double(Tickrake::Streaming::OptionContractResolver, resolve: %w[SPXW_5950C SPXW_6000C])
      resolver2 = instance_double(Tickrake::Streaming::OptionContractResolver, resolve: %w[SPXW_6000C SPXW_6050C])
      allow(Tickrake::Streaming::OptionContractResolver).to receive(:new).and_return(resolver1, resolver2)

      # 1. Window opens at 08:30 -> resolves initial contracts and registers callback block
      t_open = Time.new(2026, 9, 21, 8, 30, 0, "-05:00")
      dyn_job.sync_subscriptions(t_open)

      expect(fake_stream.added_subscriptions.size).to eq(1)
      expect(fake_stream.added_subscriptions[0]).to include(
        service: :level_one_options,
        symbols: %w[SPXW_5950C SPXW_6000C],
        has_block: true
      )

      # 2. 2 minutes later (< 5 min interval) -> no re-resolution yet
      dyn_job.sync_subscriptions(t_open + 120)
      expect(fake_stream.added_subscriptions.size).to eq(1)

      # 3. 5 minutes later -> re-resolves, adds only SPXW_6050C without a block, keeps SPXW_5950C
      dyn_job.sync_subscriptions(t_open + 300)
      expect(fake_stream.added_subscriptions.size).to eq(2)
      expect(fake_stream.added_subscriptions[1]).to include(
        service: :level_one_options,
        symbols: %w[SPXW_6050C],
        has_block: false
      )

      # 4. Window closes at 15:01 -> unsubscribes ALL accumulated contracts (5950C, 6000C, 6050C)
      t_close = Time.new(2026, 9, 21, 15, 1, 0, "-05:00")
      dyn_job.sync_subscriptions(t_close)

      expect(fake_stream.unsubbed_subscriptions.size).to eq(1)
      expect(fake_stream.unsubbed_subscriptions[0][:service]).to eq(:level_one_options)
      expect(fake_stream.unsubbed_subscriptions[0][:symbols]).to eq(%w[SPXW_5950C SPXW_6000C SPXW_6050C])
    end

    it "isolates contract resolution errors so other subscriptions keep running and retries on the next poll" do
      contracts_cfg = Tickrake::OptionContractsConfig.new(
        underlying: "SPX",
        option_root: "SPXW",
        atm_points: 50.0,
        front_n: 1,
        re_resolve_interval_minutes: 5
      )
      options_sub = Tickrake::StreamSubscription.new(
        name: "spxw_0dte",
        kind: :level_one,
        symbols: [],
        settings: Tickrake::LevelOneConfig.new(
          services: [:level_one_options],
          contracts: contracts_cfg
        ),
        windows: [
          Tickrake::SchedulerWindow.new(
            days: %w[mon tue wed thu fri],
            start_time: [8, 30],
            end_time: [15, 0]
          )
        ]
      )
      mixed_stream_config = Tickrake::StreamConfig.new(
        subscriptions: [options_sub, futures_sub, equities_sub],
        stale_timeout_seconds: 60
      )
      mixed_scheduled_job = instance_double(
        Tickrake::ScheduledJobConfig,
        name: "mixed_streams",
        provider: "schwab",
        settings: mixed_stream_config,
        universe: ["/ES", "SPY", "QQQ"]
      )
      mixed_job = described_class.new(runtime, scheduled_job: mixed_scheduled_job)
      mixed_job.instance_variable_set(:@stream, fake_stream)

      failing_resolver = instance_double(Tickrake::Streaming::OptionContractResolver)
      allow(failing_resolver).to receive(:resolve).and_raise(StandardError, "Schwab REST 503")
      succeeding_resolver = instance_double(Tickrake::Streaming::OptionContractResolver, resolve: %w[SPXW_6000C])
      allow(Tickrake::Streaming::OptionContractResolver).to receive(:new).and_return(failing_resolver, succeeding_resolver, failing_resolver)

      # 1. Poll 1: options resolution raises, but futures and equities still activate cleanly
      t_open = Time.new(2026, 9, 21, 9, 30, 0, "-05:00")
      expect { mixed_job.sync_subscriptions(t_open) }.not_to raise_error

      expect(logger).to have_received(:error).with(hash_including(
        event: "contract_resolution_error",
        subscription: "spxw_0dte"
      ))
      expect(fake_stream.added_subscriptions.map { |s| s[:service] }).to contain_exactly(:level_one_futures, :level_one_equities)

      # 2. Poll 2 (10s later): options resolution retries and succeeds
      mixed_job.sync_subscriptions(t_open + 10)
      expect(fake_stream.added_subscriptions.map { |s| s[:service] }).to contain_exactly(
        :level_one_futures, :level_one_equities, :level_one_options
      )

      # 3. Poll 3 (5m later): mid-session re-resolution raises, rescued without crashing
      expect { mixed_job.sync_subscriptions(t_open + 310) }.not_to raise_error
      expect(logger).to have_received(:error).with(hash_including(
        event: "contract_resolution_error",
        subscription: "spxw_0dte"
      )).twice
    end
  end

  describe "event routing and file writing" do
    before do
      job.instance_variable_set(:@stream, fake_stream)
      t = Time.new(2026, 9, 21, 10, 0, 0, "-05:00")
      job.sync_subscriptions(t)
    end

    it "writes received Level 1 events to dedicated EventStreamWriter" do
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

      # Check generated file in data_dir
      files = Dir.glob(File.join(data_dir, "level_one", "schwab", "**", "*.ndjson"))
      expect(files.size).to eq(1)

      lines = File.readlines(files.first)
      expect(lines.size).to eq(1)
      parsed = JSON.parse(lines.first)
      expect(parsed["symbol"]).to eq("/ES")
      expect(parsed["bid"]).to eq(4500.0)
      expect(parsed["received_at"]).to match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/)
      expect(parsed["quote_time"]).to match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/)
      expect(parsed["trade_time"]).to match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/)
    end

    it "tracks per-service delivery counters, accepted/discarded records, and last_event_at" do
      valid_entry = {
        "key" => "/ES",
        "1"   => 4500.0,
        "2"   => 4501.0
      }
      discarded_entry = {
        "key" => "/ES"
      }
      event = { "content" => [valid_entry, discarded_entry] }

      before_time = Time.now
      fake_stream.fire(:level_one_futures, event)
      job.close

      stats = job.service_stats["LEVELONE_FUTURES"]
      expect(stats[:callbacks]).to eq(1)
      expect(stats[:entries]).to eq(2)
      expect(stats[:accepted]).to eq(1)
      expect(stats[:discarded]).to eq(1)
      expect(stats[:last_event_at]).to be >= before_time

      job.send(:log_delivery_stats)
      expect(logger).to have_received(:info).with(hash_including(
        event: "stream_delivery_stats",
        service: "LEVELONE_FUTURES",
        callbacks: 1,
        entries: 2,
        accepted: 1,
        discarded: 1
      ))
    end

    it "rescues unexpected errors in handle_event without raising to the WebSocket receive loop" do
      allow(job).to receive(:handle_level_one_event).and_raise(StandardError, "disk error")

      expect { fake_stream.fire(:level_one_futures, { "content" => [] }) }.not_to raise_error
      expect(logger).to have_received(:error).with(hash_including(
        event: "event_handler_error",
        service: "LEVELONE_FUTURES",
        subscription: "futures_l1"
      ))
    end
  end

  describe "chart stream write metrics" do
    it "logs bar and CSV byte deltas, including the header only on the first append" do
      logged_events = []
      allow(logger).to receive(:info) { |payload| logged_events << payload }
      subscription = instance_double(Tickrake::StreamSubscription, name: "equity_chart_streams")
      bar = Tickrake::Data::Bar.new(
        datetime: Time.utc(2026, 10, 21, 14, 30, 0),
        open: 450.0, high: 451.0, low: 449.5, close: 450.5, volume: 100_000,
        source: "schwab", symbol: "SPY", frequency: "1min"
      )
      chart_buffers = job.instance_variable_get(:@chart_buffers)
      chart_buffers[subscription.name] = { "SPY" => [bar] }

      job.send(:flush_chart_buffer, subscription)

      path = File.join(candles_dir, "schwab", "2026", "10", "21", "1min", "SPY.csv")
      first_size = File.size(path)
      first_metric = logged_events.find { |event| event.is_a?(Hash) && event[:event] == "stream_bytes_written" }
      expect(first_metric).to include(
        dataset_type: "chart_stream",
        symbol: "SPY",
        market_date: "2026-10-21",
        delta_events: 1,
        delta_bytes: first_size
      )
      expect(File.readlines(path).first.strip).to eq("datetime,open,high,low,close,volume")

      chart_buffers[subscription.name] = { "SPY" => [bar] }
      job.send(:flush_chart_buffer, subscription)

      second_size = File.size(path)
      metrics = logged_events.select { |event| event.is_a?(Hash) && event[:event] == "stream_bytes_written" }
      expect(metrics.last).to include(dataset_type: "chart_stream", delta_events: 1, delta_bytes: second_size - first_size)
      expect(File.readlines(path).count { |line| line.start_with?("datetime,") }).to eq(1)
    end
  end

  describe "#run_session reconnect subscription state" do
    it "re-adds active in-window subscriptions to the replacement client after a disconnect" do
      client1 = FakeClient.new
      client2 = FakeClient.new
      clients = [client1, client2]

      allow(SchwabRb::Stream::Client).to receive(:new) { clients.shift || FakeClient.new }

      # In-window time: Monday 10:00 (both futures and equities active)
      monday_morning = Time.new(2026, 9, 21, 10, 0, 0, "-05:00")
      allow(Time).to receive(:now).and_return(monday_morning)

      # Make first watchdog loop trigger a disconnect by raising an error or breaking
      call_count = 0
      allow(job).to receive(:watchdog) do |stream|
        call_count += 1
        if call_count == 1
          # First session simulates a disconnect/error
          raise "simulated disconnect"
        else
          # Second session stops the job
          job.stop
        end
      end
      allow(job).to receive(:sleep) # Don't sleep during backoff in test

      job.run_session(window_start: monday_morning)

      # Verify client 1 got subscriptions
      expect(client1.added_subscriptions.map { |s| s[:service] }).to contain_exactly(:level_one_futures, :level_one_equities)

      # Verify replacement client 2 also received subscriptions
      expect(client2.added_subscriptions.map { |s| s[:service] }).to contain_exactly(:level_one_futures, :level_one_equities)
      expect(client2.callbacks.keys).to contain_exactly(:level_one_futures, :level_one_equities)
    end

    it "does not send out-of-window subscriptions during reconnect" do
      client1 = FakeClient.new
      client2 = FakeClient.new
      clients = [client1, client2]

      allow(SchwabRb::Stream::Client).to receive(:new) { clients.shift || FakeClient.new }

      # Sunday evening -> only futures active
      sunday_evening = Time.new(2026, 9, 20, 18, 0, 0, "-05:00")
      allow(Time).to receive(:now).and_return(sunday_evening)

      call_count = 0
      allow(job).to receive(:watchdog) do |stream|
        call_count += 1
        if call_count == 1
          raise "simulated disconnect"
        else
          job.stop
        end
      end
      allow(job).to receive(:sleep)

      job.run_session(window_start: sunday_evening)

      expect(client1.added_subscriptions.map { |s| s[:service] }).to eq([:level_one_futures])
      expect(client2.added_subscriptions.map { |s| s[:service] }).to eq([:level_one_futures])
      expect(client2.added_subscriptions.map { |s| s[:service] }).not_to include(:level_one_equities)
    end
  end
end
