# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::OrderBookJob do
  let(:pending_events_dir) { Dir.mktmpdir("events") }
  let(:logger) { instance_double(Logger, info: nil, warn: nil, error: nil) }
  let(:order_book_config) do
    instance_double(
      Tickrake::OrderBookConfig,
      services: ["TOTAL_VIEW"],
      rotation_interval_seconds: 300,
      options_book?: false
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
      name: "equity_order_book",
      provider: "schwab",
      settings: order_book_config,
      universe: ["SPY"]
    )
  end

  subject(:job) { described_class.new(runtime, scheduled_job: scheduled_job) }

  after { FileUtils.rm_rf(pending_events_dir) }

  describe "#handle_event (private)" do
    let(:writer) { instance_double(Tickrake::EventsWriter, write: nil, close: nil, recover_stale_files: nil) }

    before do
      allow(Tickrake::EventsWriter).to receive(:new).and_return(writer)
    end

    it "writes an order_book row with UTC ISO 8601 timestamps" do
      event = {
        "content" => [{
          "key" => "SPY",
          "1"   => 1_729_261_800_000,
          "2"   => [{ "0" => 585.12, "1" => 100 }],
          "3"   => [{ "0" => 585.14, "1" => 150 }]
        }]
      }

      job.send(:handle_event, event, service: "TOTAL_VIEW")

      expect(writer).to have_received(:write).with(hash_including(
        "job_type"    => "order_book",
        "provider"    => "schwab",
        "symbol"      => "SPY",
        "service"     => "TOTAL_VIEW",
        "received_at" => match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/),
        "book_time"   => "2024-10-18T14:30:00Z"
      ))
    end
  end
end
