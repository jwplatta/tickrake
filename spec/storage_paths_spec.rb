# frozen_string_literal: true

RSpec.describe Tickrake::Storage::Paths do
  it "writes option snapshots under provider sample-date folders while keeping filenames unchanged" do
    config = Tickrake::Config.new(
      timezone: "America/Chicago",
      sqlite_path: "/tmp/tickrake.sqlite3",
      providers: {},
      default_provider_name: "schwab",
      option_root_tickers: {},
      data_dir: "/tmp/data",
      history_dir: "/tmp/data/history",
      options_dir: "/tmp/data/options",
      max_workers: 2,
      retry_count: 1,
      retry_delay_seconds: 0,
      option_fetch_timeout_seconds: 30,
      candle_fetch_timeout_seconds: 30,
      import_jobs: [],
      jobs: []
    )

    path = described_class.new(config).option_sample_path(
      provider: "schwab",
      symbol: "$SPX",
      expiration_date: Date.new(2026, 4, 17),
      timestamp: Time.utc(2026, 4, 10, 14, 30, 0),
      root: "SPXW"
    )

    expect(path).to eq("/tmp/data/options/schwab/2026/04/10/SPXW_exp2026-04-17_2026-04-10_14-30-00.csv")
  end
end
