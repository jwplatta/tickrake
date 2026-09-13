# frozen_string_literal: true

require_relative "../spec_helper"
require "tmpdir"

RSpec.describe Tickrake::Index::RootIndexBuilder do
  def make_tracker(dir)
    Tickrake::Tracker.new(File.join(dir, "tickrake.sqlite3"))
  end

  def upsert_raw(tracker, provider:, root:, path:, expiration_date:, collection_id:, sampled_at:)
    stat_time = Time.now.to_i
    tracker.upsert_file_metadata(
      path: path,
      dataset_type: "options",
      provider_name: provider,
      ticker: root,
      storage_format: "csv",
      storage_location: "local",
      expiration_date: expiration_date,
      collection_id: collection_id,
      row_count: 510,
      first_observed_at: sampled_at,
      last_observed_at: sampled_at,
      file_mtime: stat_time,
      file_size: 512
    )
  end

  describe "#build" do
    it "produces the expected top-level JSON shape" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        builder = described_class.new(tracker: tracker, options_dir: dir)
        result = builder.build(provider: "schwab", root: "SPXW")

        expect(result["schema_version"]).to eq(1)
        expect(result["provider"]).to eq("schwab")
        expect(result["root"]).to eq("SPXW")
        expect(result).to have_key("updated_at")
        expect(result).not_to have_key("historical")
        expect(result["intraday"]).to be_nil
      end
    end

    it "builds the intraday section with one file per expiration date" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        today = Time.now.utc
        today_str = today.strftime("%Y-%m-%d")
        exp1 = today_str
        exp2 = (today + 86_400 * 7).strftime("%Y-%m-%d")
        collection_id = "options-#{today.strftime("%Y%m%dT%H%M%SZ")}"
        sampled_at = today.iso8601

        upsert_raw(tracker,
          provider: "schwab", root: "SPXW",
          path: "#{dir}/SPXW_exp#{exp1}_coll1.csv",
          expiration_date: exp1, collection_id: collection_id, sampled_at: sampled_at)
        upsert_raw(tracker,
          provider: "schwab", root: "SPXW",
          path: "#{dir}/SPXW_exp#{exp2}_coll1.csv",
          expiration_date: exp2, collection_id: collection_id, sampled_at: sampled_at)

        builder = described_class.new(tracker: tracker, options_dir: dir)
        result = builder.build(provider: "schwab", root: "SPXW")

        intraday = result["intraday"]
        expect(intraday).not_to be_nil
        expect(intraday).not_to have_key("collection_id")
        expect(intraday["sample_date"]).to eq(today_str)
        expect(intraday["status"]).to eq("complete")
        expect(intraday["files"].length).to eq(2)
        expect(intraday["files"].map { |f| f["expiration_date"] }).to contain_exactly(exp1, exp2)
        expect(intraday["files"].first["uri"]).to start_with("file://")
      end
    end

    it "merges intraday files from multiple collections, picking latest per expiration" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        today = Time.now.utc
        today_str = today.strftime("%Y-%m-%d")
        exp_0dte = today_str
        exp_7dte = (today + 86_400 * 7).strftime("%Y-%m-%d")
        exp_15dte = (today + 86_400 * 15).strftime("%Y-%m-%d")

        t1 = (today - 600).utc.iso8601
        t2 = today.utc.iso8601

        # job 1 ran first: 0DTE + 7DTE
        coll1 = "options-#{(today - 600).strftime("%Y%m%dT%H%M%SZ")}"
        upsert_raw(tracker, provider: "schwab", root: "SPXW",
          path: "#{dir}/SPXW_exp#{exp_0dte}_coll1.csv",
          expiration_date: exp_0dte, collection_id: coll1, sampled_at: t1)
        upsert_raw(tracker, provider: "schwab", root: "SPXW",
          path: "#{dir}/SPXW_exp#{exp_7dte}_coll1.csv",
          expiration_date: exp_7dte, collection_id: coll1, sampled_at: t1)

        # job 2 ran later: 15DTE only
        coll2 = "options-#{today.strftime("%Y%m%dT%H%M%SZ")}"
        upsert_raw(tracker, provider: "schwab", root: "SPXW",
          path: "#{dir}/SPXW_exp#{exp_15dte}_coll2.csv",
          expiration_date: exp_15dte, collection_id: coll2, sampled_at: t2)

        builder = described_class.new(tracker: tracker, options_dir: dir)
        result = builder.build(provider: "schwab", root: "SPXW")

        intraday = result["intraday"]
        expect(intraday).not_to be_nil
        expect(intraday["files"].length).to eq(3)
        exps = intraday["files"].map { |f| f["expiration_date"] }
        expect(exps).to contain_exactly(exp_0dte, exp_7dte, exp_15dte)
      end
    end

    it "returns intraday null when no raw options rows exist" do
      Dir.mktmpdir do |dir|
        tracker = make_tracker(dir)
        builder = described_class.new(tracker: tracker, options_dir: dir)
        result = builder.build(provider: "schwab", root: "SPXW")
        expect(result["intraday"]).to be_nil
      end
    end

  end
end
