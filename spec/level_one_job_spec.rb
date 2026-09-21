# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::LevelOneJob do
  let(:pending_events_dir) { Dir.mktmpdir("events") }
  let(:logger) { instance_double(Logger, info: nil, warn: nil, error: nil) }
  let(:level_one_config) do
    instance_double(
      Tickrake::LevelOneConfig,
      services: [:level_one_futures],
      rotation_interval_seconds: 300
    )
  end
  let(:config) do
    instance_double(Tickrake::Config, pending_events_dir: pending_events_dir)
  end
  let(:runtime) do
    instance_double(Tickrake::Runtime, config: config, logger: logger)
  end
  let(:scheduled_job) do
    instance_double(
      Tickrake::ScheduledJobConfig,
      name: "futures_level_one",
      provider: "schwab",
      settings: level_one_config,
      universe: ["/ES"]
    )
  end

  subject(:job) { described_class.new(runtime, scheduled_job: scheduled_job) }

  after { FileUtils.rm_rf(pending_events_dir) }

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Build a minimal fake stream that calls the registered callback once then
  # goes silent. The callback is captured via #on and invoked via #fire.
  class FakeStream
    attr_reader :started, :stopped

    def initialize
      @callbacks = {}
      @started   = false
      @stopped   = false
    end

    def on(service_sym, symbols:, fields:, &block)
      @callbacks[service_sym] = block
    end

    def start_async
      @started = true
    end

    def stop
      @stopped = true
    end

    def fire(service_sym, event)
      @callbacks[service_sym]&.call(event)
    end
  end

  def futures_event(symbol: "/ES", bid: 4500.0, ask: 4501.0)
    {
      "content" => [{
        "key"  => symbol,
        "1"    => bid,
        "2"    => ask,
        "3"    => 4500.5,
        "10"   => (Time.now.to_f * 1000).to_i,
        "11"   => (Time.now.to_f * 1000).to_i
      }]
    }
  end

  # ---------------------------------------------------------------------------
  # handle_event
  # ---------------------------------------------------------------------------

  describe "#handle_event (private)" do
    let(:writer) { instance_double(Tickrake::EventsWriter, write: nil, close: nil, recover_stale_files: nil) }

    before do
      allow(Tickrake::EventsWriter).to receive(:new).and_return(writer)
    end

    it "writes a level_one row for a valid LEVELONE_FUTURES event" do
      job.send(:handle_event, futures_event, service: "LEVELONE_FUTURES")

      expect(writer).to have_received(:write).with(hash_including(
        "job_type" => "level_one",
        "symbol"   => "/ES",
        "service"  => "LEVELONE_FUTURES",
        "bid"      => 4500.0,
        "ask"      => 4501.0
      ))
    end

    it "skips entries that carry no price fields" do
      empty_event = { "content" => [{ "key" => "/ES" }] }
      job.send(:handle_event, empty_event, service: "LEVELONE_FUTURES")
      expect(writer).not_to have_received(:write)
    end

    it "skips entries with no key" do
      keyless_event = { "content" => [{ "1" => 4500.0 }] }
      job.send(:handle_event, keyless_event, service: "LEVELONE_FUTURES")
      expect(writer).not_to have_received(:write)
    end

    it "updates last_event_at using local time, not quote timestamp" do
      before_call = Time.now
      job.send(:handle_event, futures_event, service: "LEVELONE_FUTURES")
      after_call  = Time.now

      last = job.instance_variable_get(:@last_event_mu).synchronize do
        job.instance_variable_get(:@last_event_at)
      end

      expect(last).to be >= before_call
      expect(last).to be <= after_call
    end
  end

  # ---------------------------------------------------------------------------
  # Watchdog — stale detection triggers reconnect
  # ---------------------------------------------------------------------------

  describe "#run_session watchdog" do
    let(:fake_stream) { FakeStream.new }
    let(:writer) { instance_double(Tickrake::EventsWriter, write: nil, close: nil, recover_stale_files: nil) }

    before do
      allow(Tickrake::EventsWriter).to receive(:new).and_return(writer)
      allow(Tickrake::ClientFactory).to receive(:new).and_return(double(build: nil))

      call_count = 0
      allow(SchwabRb::Stream::Client).to receive(:new) do
        call_count += 1
        if call_count == 1
          fake_stream
        else
          inert = FakeStream.new
          job.stop
          inert
        end
      end

      # Shrink timeouts so the spec runs in milliseconds
      stub_const("Tickrake::LevelOneJob::WATCHDOG_POLL_SECONDS", 0.01)
      stub_const("Tickrake::LevelOneJob::DEFAULT_STALE_TIMEOUT_SECONDS", 0.05)
      stub_const("Tickrake::LevelOneJob::RECONNECT_INITIAL_DELAY", 0)
    end

    it "reconnects after the stale timeout when no events arrive" do
      thread = Thread.new { job.run_session(window_start: Time.now) }
      thread.join(3)

      expect(fake_stream.stopped).to be true
      expect(logger).to have_received(:warn).with(hash_including(event: "stream_stale"))
      expect(logger).to have_received(:info).with(hash_including(event: "reconnect_attempt"))
    end

    it "does not reconnect when events keep arriving within the timeout" do
      stub_const("Tickrake::LevelOneJob::DEFAULT_STALE_TIMEOUT_SECONDS", 10)
      thread = Thread.new do
        job.run_session(window_start: Time.now)
      end

      # Fire events continuously for a short window, then stop
      5.times do
        fake_stream.fire(:level_one_futures, futures_event)
        sleep(0.01)
      end
      job.stop
      thread.join(3)

      expect(logger).not_to have_received(:warn).with(hash_including(event: "stream_stale"))
    end
  end

  # ---------------------------------------------------------------------------
  # Normal stop
  # ---------------------------------------------------------------------------

  describe "#stop" do
    it "causes run_session to exit without reconnecting" do
      writer = instance_double(Tickrake::EventsWriter, write: nil, close: nil, recover_stale_files: nil)
      allow(Tickrake::EventsWriter).to receive(:new).and_return(writer)
      allow(Tickrake::ClientFactory).to receive(:new).and_return(double(build: nil))

      stream = FakeStream.new
      allow(SchwabRb::Stream::Client).to receive(:new).and_return(stream)

      stub_const("Tickrake::LevelOneJob::WATCHDOG_POLL_SECONDS", 0.01)
      stub_const("Tickrake::LevelOneJob::DEFAULT_STALE_TIMEOUT_SECONDS", 10)

      thread = Thread.new { job.run_session(window_start: Time.now) }
      sleep(0.05)
      job.stop
      thread.join(2)

      expect(SchwabRb::Stream::Client).to have_received(:new).once
    end
  end
end
