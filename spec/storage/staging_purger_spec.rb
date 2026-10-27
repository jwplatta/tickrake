# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::Storage::StagingPurger do
  let(:datastore_config) do
    Tickrake::DatastoreConfig.new(
      name: "staging_target",
      type: "s3_compatible",
      bucket: "tickrake-staging",
      endpoint: "http://localhost:9000",
      access_key_id: "minioadmin",
      secret_access_key: "minioadmin"
    )
  end

  let(:config) do
    instance_double(
      Tickrake::Config,
      data_dir: "/tmp/tickrake_data",
      staging_target: datastore_config
    )
  end

  let(:logger) { instance_double(Logger, info: nil, warn: nil, error: nil) }
  let(:runtime) { instance_double(Tickrake::Runtime, config: config, logger: logger) }
  let(:object_store_client) { instance_double(Tickrake::Storage::ObjectStoreClient, bucket: "tickrake-staging") }

  before do
    allow(Tickrake::Storage::ObjectStoreClient).to receive(:new).and_return(object_store_client)
  end

  describe "#purge" do
    it "purges remote keys for a specific dataset and date" do
      keys = [
        "order_book/schwab/2026/09/16/SPY.ndjson",
        "order_book/schwab/2026/09/16/QQQ.ndjson",
        "order_book/schwab/2026/09/17/SPY.ndjson"
      ]

      allow(object_store_client).to receive(:list_keys).with(prefix: "order_book/schwab/2026/09/16/").and_return(
        [
          "order_book/schwab/2026/09/16/SPY.ndjson",
          "order_book/schwab/2026/09/16/QQQ.ndjson"
        ]
      )
      allow(object_store_client).to receive(:delete_keys)

      purger = described_class.new(runtime: runtime)
      summary = purger.purge(
        dataset_types: ["order_book"],
        date: "2026-09-16",
        provider: "schwab",
        purge_remote: true,
        purge_local: false
      )

      expect(object_store_client).to have_received(:delete_keys).with([
        "order_book/schwab/2026/09/16/SPY.ndjson",
        "order_book/schwab/2026/09/16/QQQ.ndjson"
      ])
      expect(summary[:remote_keys_scanned]).to eq(2)
      expect(summary[:remote_keys_deleted]).to eq(2)
      expect(summary[:local_files_scanned]).to eq(0)
    end

    it "respects dry_run mode without deleting remote keys" do
      allow(object_store_client).to receive(:list_keys).and_return(["options/schwab/2026/10/18/SPY_exp2026-10-18.csv"])
      allow(object_store_client).to receive(:delete_keys)

      purger = described_class.new(runtime: runtime)
      summary = purger.purge(
        dataset_types: ["options"],
        date: Date.new(2026, 10, 18),
        provider: "schwab",
        dry_run: true,
        purge_remote: true,
        purge_local: false
      )

      expect(object_store_client).not_to have_received(:delete_keys)
      expect(summary[:remote_keys_scanned]).to eq(1)
      expect(summary[:remote_keys_deleted]).to eq(0)
      expect(summary[:dry_run]).to be true
    end

    it "purges local files when purge_local is true" do
      Dir.mktmpdir do |dir|
        allow(Tickrake::PathSupport).to receive(:expand_path).with("/tmp/tickrake_data").and_return(dir)

        order_book_dir = File.join(dir, "order_book", "schwab", "2026", "09", "16")
        FileUtils.mkdir_p(order_book_dir)
        test_file = File.join(order_book_dir, "SPY.ndjson")
        File.write(test_file, '{"test":1}')

        allow(object_store_client).to receive(:list_keys).and_return([])

        purger = described_class.new(runtime: runtime)
        summary = purger.purge(
          dataset_types: ["order_book"],
          date: "2026-09-16",
          provider: "schwab",
          purge_remote: false,
          purge_local: true
        )

        expect(File.exist?(test_file)).to be false
        expect(summary[:local_files_scanned]).to eq(1)
        expect(summary[:local_files_deleted]).to eq(1)
      end
    end
  end
end
