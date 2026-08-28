# frozen_string_literal: true

require_relative "../spec_helper"
require "tmpdir"
require "json"

RSpec.describe Tickrake::Index::IntradayPublisher do
  def make_tracker(dir)
    Tickrake::Tracker.new(File.join(dir, "tickrake.sqlite3"))
  end

  def upsert_raw(tracker, dir, provider:, root:, expiration_date:, collection_id:)
    sampled_at = Time.now.utc.iso8601
    path = File.join(dir, "#{root}_exp#{expiration_date}_#{collection_id}.csv")
    tracker.upsert_file_metadata(
      path: path,
      dataset_type: "options",
      provider_name: provider,
      ticker: root,
      storage_format: "csv",
      storage_location: "local",
      expiration_date: expiration_date,
      collection_id: collection_id,
      row_count: 500,
      first_observed_at: sampled_at,
      last_observed_at: sampled_at,
      file_mtime: Time.now.to_i,
      file_size: 512
    )
  end

  let(:logger) { Logger.new(nil) }

  describe "#publish" do
    it "writes ROOT.json when received count matches expected" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        collection_id = "options-#{Time.now.utc.strftime("%Y%m%dT%H%M%SZ")}"

        upsert_raw(tracker, dir, provider: "schwab", root: "SPXW",
          expiration_date: "2026-08-27", collection_id: collection_id)
        upsert_raw(tracker, dir, provider: "schwab", root: "SPXW",
          expiration_date: "2026-08-28", collection_id: collection_id)

        described_class.new(tracker: tracker, options_dir: dir, logger: logger)
          .publish(
            collection_id: collection_id,
            expected_counts: { ["schwab", "SPXW"] => 2 }
          )

        root_json = File.join(dir, "schwab", "SPXW.json")
        expect(File.exist?(root_json)).to be true

        payload = JSON.parse(File.read(root_json))
        expect(payload["intraday"]).not_to be_nil
        expect(payload["intraday"]).not_to have_key("collection_id")
        expect(payload["intraday"]["files"].length).to eq(2)
      end
    end

    it "does not write ROOT.json when received count is less than expected" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        collection_id = "options-#{Time.now.utc.strftime("%Y%m%dT%H%M%SZ")}"

        upsert_raw(tracker, dir, provider: "schwab", root: "SPXW",
          expiration_date: "2026-08-27", collection_id: collection_id)

        described_class.new(tracker: tracker, options_dir: dir, logger: logger)
          .publish(
            collection_id: collection_id,
            expected_counts: { ["schwab", "SPXW"] => 3 }
          )

        expect(File.exist?(File.join(dir, "schwab", "SPXW.json"))).to be false
      end
    end

    it "publishes independently per root" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        collection_id = "options-#{Time.now.utc.strftime("%Y%m%dT%H%M%SZ")}"

        # SPXW complete (1 of 1)
        upsert_raw(tracker, dir, provider: "schwab", root: "SPXW",
          expiration_date: "2026-08-27", collection_id: collection_id)
        # SPY incomplete (0 of 2)

        described_class.new(tracker: tracker, options_dir: dir, logger: logger)
          .publish(
            collection_id: collection_id,
            expected_counts: { ["schwab", "SPXW"] => 1, ["schwab", "SPY"] => 2 }
          )

        expect(File.exist?(File.join(dir, "schwab", "SPXW.json"))).to be true
        expect(File.exist?(File.join(dir, "schwab", "SPY.json"))).to be false
      end
    end

    it "does not raise when Publisher raises — caller handles it" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        collection_id = "options-#{Time.now.utc.strftime("%Y%m%dT%H%M%SZ")}"

        upsert_raw(tracker, dir, provider: "schwab", root: "SPXW",
          expiration_date: "2026-08-27", collection_id: collection_id)

        allow_any_instance_of(Tickrake::Index::Publisher).to receive(:publish)
          .and_raise(Tickrake::Error, "simulated failure")

        expect {
          described_class.new(tracker: tracker, options_dir: dir, logger: logger)
            .publish(
              collection_id: collection_id,
              expected_counts: { ["schwab", "SPXW"] => 1 }
            )
        }.to raise_error(Tickrake::Error)
      end
    end
  end
end
