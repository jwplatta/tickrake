# frozen_string_literal: true

RSpec.describe Tickrake::Storage::S3Archive do
  def build_config(dir, prefix: "", storage_class: "GLACIER_IR")
    Tickrake::Config.new(
      timezone: "America/Chicago",
      sqlite_path: File.join(dir, "tickrake.sqlite3"),
      providers: {
        "schwab" => Tickrake::ProviderDefinition.new(name: "schwab", adapter: "schwab", settings: {}, symbol_map: {})
      },
      default_provider_name: "schwab",
      option_root_tickers: { "SPXW" => "SPX" },
      option_snapshot_filename_timezone: "utc",
      s3_archive: Tickrake::S3ArchiveConfig.new(
        bucket: "tickrake",
        region: "us-east-1",
        prefix: prefix,
        storage_class: storage_class
      ),
      data_dir: File.join(dir, "data"),
      history_dir: File.join(dir, "data", "history"),
      options_dir: File.join(dir, "data", "options"),
      max_workers: 2,
      retry_count: 1,
      retry_delay_seconds: 0,
      option_fetch_timeout_seconds: 30,
      candle_fetch_timeout_seconds: 30,
      import_jobs: [],
      jobs: []
    )
  end

  it "maps local data paths to matching s3 keys" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      path = File.join(config.data_dir, "options", "schwab", "2026", "06", "26", "SPXW_samples_2026-06-26.csv")
      archive = described_class.new(config, s3_client: instance_double(Aws::S3::Client))

      expect(archive.key_for(path)).to eq("options/schwab/2026/06/26/SPXW_samples_2026-06-26.csv")
      expect(archive.uri_for(path)).to eq("s3://tickrake/options/schwab/2026/06/26/SPXW_samples_2026-06-26.csv")
    end
  end

  it "prepends the configured s3 prefix" do
    Dir.mktmpdir do |dir|
      config = build_config(dir, prefix: "/archive/root/")
      path = File.join(config.data_dir, "options", "schwab", "2026", "06", "26", "SPXW_samples_2026-06-26.csv")
      archive = described_class.new(config, s3_client: instance_double(Aws::S3::Client))

      expect(archive.key_for(path)).to eq("archive/root/options/schwab/2026/06/26/SPXW_samples_2026-06-26.csv")
    end
  end

  it "uploads with the configured storage class" do
    Dir.mktmpdir do |dir|
      config = build_config(dir, storage_class: "GLACIER_IR")
      path = File.join(config.data_dir, "options", "schwab", "2026", "06", "26", "SPXW_samples_2026-06-26.csv")
      FileUtils.mkdir_p(File.dirname(path))
      File.write(path, "abc")
      client = instance_double(Aws::S3::Client, put_object: true)
      archive = described_class.new(config, s3_client: client)

      archive.upload(path)

      expect(client).to have_received(:put_object).with(
        hash_including(
          bucket: "tickrake",
          key: "options/schwab/2026/06/26/SPXW_samples_2026-06-26.csv",
          storage_class: "GLACIER_IR"
        )
      )
    end
  end

  it "verifies remote existence and size with head_object" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      path = File.join(config.data_dir, "options", "schwab", "2026", "06", "26", "SPXW_samples_2026-06-26.parquet")
      FileUtils.mkdir_p(File.dirname(path))
      File.write(path, "parquet")
      client = instance_double(Aws::S3::Client)
      allow(client).to receive(:head_object).and_return(instance_double(Aws::S3::Types::HeadObjectOutput, content_length: 7))
      archive = described_class.new(config, s3_client: client)

      remote_object = archive.verify(path)

      expect(client).to have_received(:head_object).with(
        bucket: "tickrake",
        key: "options/schwab/2026/06/26/SPXW_samples_2026-06-26.parquet"
      )
      expect(remote_object.uri).to eq("s3://tickrake/options/schwab/2026/06/26/SPXW_samples_2026-06-26.parquet")
      expect(remote_object.size).to eq(7)
    end
  end
end
