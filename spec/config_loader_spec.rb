# frozen_string_literal: true

RSpec.describe Tickrake::ConfigLoader do
  it "loads the example config" do
    config = described_class.load(File.expand_path("../config/tickrake.example.yml", __dir__))

    expect(config.options_monitor_interval_seconds).to eq(300)
    expect(config.dte_buckets).to include(0, 10, 30)
    expect(config.sqlite_path).to eq(File.expand_path("~/.tickrake/tickrake.sqlite3"))
    expect(config.options_universe.map(&:symbol)).to include("$SPX", "SPY")
    expect(config.candles_universe.map(&:frequency)).to all(eq("day"))
  end

  it "rejects malformed dte buckets" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "bad.yml")
      File.write(path, <<~YAML)
        schedule:
          options_monitor:
            interval_seconds: 300
            windows:
              - days: [mon]
                start: "08:30"
                end: "15:00"
          eod_candles:
            run_at: "16:10"
            days: [mon]
        options:
          dte_buckets: [near]
          universe:
            - symbol: SPY
        candles:
          universe:
            - symbol: SPY
              start_date: "2020-01-01"
      YAML

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /Invalid DTE bucket/)
    end
  end
end
