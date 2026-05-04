# frozen_string_literal: true

RSpec.describe Tickrake::DataLoader do
  def build_config(history_dir:, options_dir:, sqlite_path:)
    Tickrake::Config.new(
      timezone: "America/Chicago",
      sqlite_path: sqlite_path,
      providers: {
        "ibkr-paper" => Tickrake::ProviderDefinition.new(name: "ibkr-paper", adapter: "ibkr", settings: {}, symbol_map: {}),
        "schwab" => Tickrake::ProviderDefinition.new(name: "schwab", adapter: "schwab", settings: {}, symbol_map: {})
      },
      default_provider_name: "ibkr-paper",
      option_root_tickers: { "SPXW" => "SPX" },
      data_dir: File.dirname(history_dir),
      history_dir: history_dir,
      options_dir: options_dir,
      max_workers: 2,
      retry_count: 1,
      retry_delay_seconds: 1,
      option_fetch_timeout_seconds: 30,
      candle_fetch_timeout_seconds: 30,
      import_jobs: [],
      jobs: [
        Tickrake::ScheduledJobConfig.new(
          name: "options",
          type: "options",
          interval_seconds: 300,
          windows: [Tickrake::SchedulerWindow.new(days: %w[mon tue wed thu fri], start_time: [8, 30], end_time: [15, 0])],
          run_at: nil,
          days: [],
          lookback_days: nil,
          dte_buckets: [0, 1],
          universe: [
            Tickrake::OptionSymbol.new(symbol: "$SPX", option_root: "SPXW"),
            Tickrake::OptionSymbol.new(symbol: "SPY", option_root: nil)
          ]
        ),
        Tickrake::ScheduledJobConfig.new(
          name: "candles",
          type: "candles",
          interval_seconds: nil,
          windows: [],
          run_at: [16, 10],
          days: %w[mon tue wed thu fri],
          lookback_days: 7,
          dte_buckets: [],
          universe: [
            Tickrake::CandleSymbol.new(
              symbol: "SPY",
              frequencies: ["1min"],
              start_date: Date.iso8601("2026-01-01"),
              need_extended_hours_data: false,
              need_previous_close: false
            )
          ]
        )
      ]
    )
  end

  it "builds from config_path without requiring callers to construct config or runtime" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "tickrake.yml")
      File.write(path, <<~YAML)
        default_provider: ibkr-paper
        providers:
          ibkr-paper:
            adapter: ibkr
        schedule:
          manual_candles:
            type: candles
            manual: true
            provider: ibkr-paper
            lookback_days: 7
            universe:
              - symbol: SPY
                start_date: "2026-01-01"
                frequencies: [1min]
      YAML

      loader = described_class.new(config_path: path)

      expect(loader).to be_a(described_class)
    end
  end

  it "loads candles as typed streamed hashes with row-level date filtering by default" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      sqlite_path = File.join(dir, "tickrake.sqlite3")
      provider_dir = File.join(history_dir, "ibkr-paper")
      FileUtils.mkdir_p(provider_dir)
      path = File.join(provider_dir, "SPY_1min.csv")
      File.write(path, <<~CSV)
        datetime,open,high,low,close,volume
        2026-04-09T23:59:00Z,1,2,0.5,1.5,10
        2026-04-10T13:30:00Z,2,3,1.5,2.5,11
        2026-04-11T13:31:00Z,3,4,2.5,3.5,12
      CSV

      config = build_config(history_dir: history_dir, options_dir: options_dir, sqlite_path: sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      loader = described_class.new(config: config, tracker: tracker)

      rows = loader.load_candles(
        provider: "ibkr-paper",
        ticker: "SPY",
        frequency: "minute",
        start_date: Date.iso8601("2026-04-10"),
        end_date: Date.iso8601("2026-04-10")
      ).to_a

      expect(rows.length).to eq(1)
      expect(rows.first).to include(
        "datetime" => Time.iso8601("2026-04-10T13:30:00Z"),
        "open" => 2.0,
        "high" => 3.0,
        "close" => 2.5,
        "volume" => 11
      )
      expect(rows.first).not_to have_key("metadata")
    end
  end

  it "yields candle rows without full materialization" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      sqlite_path = File.join(dir, "tickrake.sqlite3")
      provider_dir = File.join(history_dir, "ibkr-paper")
      FileUtils.mkdir_p(provider_dir)
      path = File.join(provider_dir, "SPY_1min.csv")
      File.write(path, <<~CSV)
        datetime,open,high,low,close,volume
        2026-04-10T13:30:00Z,1,2,0.5,1.5,10
        2026-04-10T13:31:00Z,2,3,1.5,2.5,11
      CSV

      config = build_config(history_dir: history_dir, options_dir: options_dir, sqlite_path: sqlite_path)
      loader = described_class.new(config: config, tracker: Tickrake::Tracker.new(config.sqlite_path))

      enumerator = loader.load_candles(
        provider: "ibkr-paper",
        ticker: "SPY",
        frequency: "1min",
        start_date: Date.iso8601("2026-04-10"),
        end_date: Date.iso8601("2026-04-10")
      )

      expect(enumerator).to be_a(Enumerator)
      expect(enumerator.next.fetch("datetime")).to eq(Time.iso8601("2026-04-10T13:30:00Z"))
    end
  end

  it "loads option chains as typed plain hashes by default using canonical ticker aliases" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      sqlite_path = File.join(dir, "tickrake.sqlite3")
      provider_dir = File.join(options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      first_path = File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-30-00.csv")
      second_path = File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-35-00.csv")
      File.write(first_path, "contract_type,symbol,description,strike,bid,ask,expiration_date,open_interest\nCALL,SPXW,first,5100,1.25,1.35,2026-04-17,42\n")
      File.write(second_path, "contract_type,symbol,description,strike,bid,ask,expiration_date,open_interest\nCALL,SPXW,second,5105,1.45,1.6,2026-04-17,55\n")

      config = build_config(history_dir: history_dir, options_dir: options_dir, sqlite_path: sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      tracker.bulk_upsert_file_metadata(
        [
          {
            path: first_path,
            dataset_type: "options",
            provider_name: "schwab",
            ticker: "SPXW",
            frequency: nil,
            expiration_date: "2026-04-17",
            row_count: 1,
            first_observed_at: "2026-04-10T14:30:00Z",
            last_observed_at: "2026-04-10T14:30:00Z",
            file_mtime: 1,
            file_size: 46
          },
          {
            path: second_path,
            dataset_type: "options",
            provider_name: "schwab",
            ticker: "SPXW",
            frequency: nil,
            expiration_date: "2026-04-17",
            row_count: 1,
            first_observed_at: "2026-04-10T14:35:00Z",
            last_observed_at: "2026-04-10T14:35:00Z",
            file_mtime: 1,
            file_size: 47
          }
        ]
      )
      loader = described_class.new(config: config, tracker: tracker)

      rows = loader.load_option_chains(
        provider: "schwab",
        ticker: "$SPX",
        expiration_date: Date.iso8601("2026-04-17"),
        start_date: Date.iso8601("2026-04-10"),
        end_date: Date.iso8601("2026-04-10")
      ).to_a

      expect(rows.map { |row| row.fetch("description") }).to eq(%w[first second])
      expect(rows.first).to include(
        "contract_type" => "CALL",
        "symbol" => "SPXW",
        "description" => "first",
        "strike" => 5100.0,
        "bid" => 1.25,
        "ask" => 1.35,
        "expiration_date" => Date.iso8601("2026-04-17"),
        "open_interest" => 42
      )
      expect(rows.first).not_to have_key("metadata")
    end
  end

  it "includes typed option snapshot metadata under a dedicated metadata key when requested" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      sqlite_path = File.join(dir, "tickrake.sqlite3")
      provider_dir = File.join(options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      path = File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-30-00.csv")
      File.write(path, "contract_type,symbol,description,delta,bid_size,expiration_date\nCALL,SPXW,first,0.35,10,2026-04-17\n")

      config = build_config(history_dir: history_dir, options_dir: options_dir, sqlite_path: sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      tracker.upsert_file_metadata(
        path: path,
        dataset_type: "options",
        provider_name: "schwab",
        ticker: "SPXW",
        frequency: nil,
        expiration_date: "2026-04-17",
        row_count: 1,
        first_observed_at: "2026-04-10T14:30:00Z",
        last_observed_at: "2026-04-10T14:30:00Z",
        file_mtime: 1,
        file_size: 46
      )
      loader = described_class.new(config: config, tracker: tracker)

      row = loader.load_option_chains(
        provider: "schwab",
        ticker: "$SPX",
        expiration_date: Date.iso8601("2026-04-17"),
        start_date: Date.iso8601("2026-04-10"),
        end_date: Date.iso8601("2026-04-10"),
        include_metadata: true
      ).first

      expect(row.fetch("metadata")).to eq(
        "dataset_type" => "options",
        "provider_name" => "schwab",
        "ticker" => "SPX",
        "option_root" => "SPXW",
        "source_path" => path,
        "sampled_at" => Time.iso8601("2026-04-10T14:30:00Z"),
        "expiration_date" => Date.iso8601("2026-04-17")
      )
      expect(row.fetch("delta")).to eq(0.35)
      expect(row.fetch("bid_size")).to eq(10)
      expect(row.fetch("expiration_date")).to eq(Date.iso8601("2026-04-17"))
    end
  end

  it "coerces blank numeric option fields to nil" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      sqlite_path = File.join(dir, "tickrake.sqlite3")
      provider_dir = File.join(options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      path = File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-30-00.csv")
      File.write(path, "contract_type,symbol,bid,ask,last_size,expiration_date\nCALL,SPXW,,, ,2026-04-17\n")

      config = build_config(history_dir: history_dir, options_dir: options_dir, sqlite_path: sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      tracker.upsert_file_metadata(
        path: path,
        dataset_type: "options",
        provider_name: "schwab",
        ticker: "SPXW",
        frequency: nil,
        expiration_date: "2026-04-17",
        row_count: 1,
        first_observed_at: "2026-04-10T14:30:00Z",
        last_observed_at: "2026-04-10T14:30:00Z",
        file_mtime: 1,
        file_size: 46
      )
      loader = described_class.new(config: config, tracker: tracker)

      row = loader.load_option_chains(
        provider: "schwab",
        ticker: "$SPX",
        expiration_date: Date.iso8601("2026-04-17"),
        start_date: Date.iso8601("2026-04-10"),
        end_date: Date.iso8601("2026-04-10")
      ).first

      expect(row.fetch("bid")).to be_nil
      expect(row.fetch("ask")).to be_nil
      expect(row.fetch("last_size")).to be_nil
    end
  end

  it "supports option-root filtering and last-in-bucket synthetic frequencies" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      sqlite_path = File.join(dir, "tickrake.sqlite3")
      provider_dir = File.join(options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      paths = [
        File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-30-00.csv"),
        File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-34-00.csv"),
        File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-39-00.csv"),
        File.join(provider_dir, "SPY_exp2026-04-17_2026-04-10_14-31-00.csv")
      ]
      %w[a b c spy].each_with_index do |description, index|
        File.write(paths[index], "contract_type,symbol,description\nCALL,#{index == 3 ? 'SPY' : 'SPXW'},#{description}\n")
      end

      config = build_config(history_dir: history_dir, options_dir: options_dir, sqlite_path: sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      tracker.bulk_upsert_file_metadata(
        [
          {
            path: paths[0],
            dataset_type: "options",
            provider_name: "schwab",
            ticker: "SPXW",
            frequency: nil,
            expiration_date: "2026-04-17",
            row_count: 1,
            first_observed_at: "2026-04-10T14:30:00Z",
            last_observed_at: "2026-04-10T14:30:00Z",
            file_mtime: 1,
            file_size: 42
          },
          {
            path: paths[1],
            dataset_type: "options",
            provider_name: "schwab",
            ticker: "SPXW",
            frequency: nil,
            expiration_date: "2026-04-17",
            row_count: 1,
            first_observed_at: "2026-04-10T14:34:00Z",
            last_observed_at: "2026-04-10T14:34:00Z",
            file_mtime: 1,
            file_size: 42
          },
          {
            path: paths[2],
            dataset_type: "options",
            provider_name: "schwab",
            ticker: "SPXW",
            frequency: nil,
            expiration_date: "2026-04-17",
            row_count: 1,
            first_observed_at: "2026-04-10T14:39:00Z",
            last_observed_at: "2026-04-10T14:39:00Z",
            file_mtime: 1,
            file_size: 42
          },
          {
            path: paths[3],
            dataset_type: "options",
            provider_name: "schwab",
            ticker: "SPY",
            frequency: nil,
            expiration_date: "2026-04-17",
            row_count: 1,
            first_observed_at: "2026-04-10T14:31:00Z",
            last_observed_at: "2026-04-10T14:31:00Z",
            file_mtime: 1,
            file_size: 43
          }
        ]
      )
      loader = described_class.new(config: config, tracker: tracker)

      rows = loader.load_option_chains(
        provider: "schwab",
        ticker: "$SPX",
        option_root: "SPXW",
        expiration_date: Date.iso8601("2026-04-17"),
        start_date: Date.iso8601("2026-04-10"),
        end_date: Date.iso8601("2026-04-10"),
        frequency: "5min"
      ).to_a

      expect(rows.map { |row| row.fetch("description") }).to eq(%w[b c])
    end
  end

  it "returns empty enumerators cleanly for unmatched searches" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      sqlite_path = File.join(dir, "tickrake.sqlite3")
      config = build_config(history_dir: history_dir, options_dir: options_dir, sqlite_path: sqlite_path)
      loader = described_class.new(config: config, tracker: Tickrake::Tracker.new(config.sqlite_path))

      expect(
        loader.load_candles(
          provider: "ibkr-paper",
          ticker: "SPY",
          frequency: "1min",
          start_date: Date.iso8601("2026-04-10"),
          end_date: Date.iso8601("2026-04-10")
        ).to_a
      ).to eq([])
    end
  end

  it "orders candle rows by sample time when requested" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      sqlite_path = File.join(dir, "tickrake.sqlite3")
      provider_dir = File.join(history_dir, "ibkr-paper")
      FileUtils.mkdir_p(provider_dir)
      path = File.join(provider_dir, "SPY_1min.csv")
      File.write(path, <<~CSV)
        datetime,open,high,low,close,volume
        2026-04-10T13:32:00Z,3,4,2.5,3.5,12
        2026-04-10T13:30:00Z,1,2,0.5,1.5,10
        2026-04-10T13:31:00Z,2,3,1.5,2.5,11
      CSV

      config = build_config(history_dir: history_dir, options_dir: options_dir, sqlite_path: sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      loader = described_class.new(config: config, tracker: tracker)

      rows = loader.load_candles(
        provider: "ibkr-paper",
        ticker: "SPY",
        frequency: "minute",
        start_date: Date.iso8601("2026-04-10"),
        end_date: Date.iso8601("2026-04-10"),
        order: :sample_time_asc
      ).to_a

      expect(rows.map { |row| row.fetch("datetime") }).to eq([
        Time.iso8601("2026-04-10T13:30:00Z"),
        Time.iso8601("2026-04-10T13:31:00Z"),
        Time.iso8601("2026-04-10T13:32:00Z")
      ])
    end
  end

  it "orders option chains by sample time when requested across multiple sample dates" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      sqlite_path = File.join(dir, "tickrake.sqlite3")
      provider_dir = File.join(options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      first_path = File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-35-00.csv")
      second_path = File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-30-00.csv")
      third_path = File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-11_14-30-00.csv")
      File.write(first_path, "contract_type,symbol,description,expiration_date\nCALL,SPXW,second,2026-04-17\n")
      File.write(second_path, "contract_type,symbol,description,expiration_date\nCALL,SPXW,first,2026-04-17\n")
      File.write(third_path, "contract_type,symbol,description,expiration_date\nCALL,SPXW,third,2026-04-17\n")

      config = build_config(history_dir: history_dir, options_dir: options_dir, sqlite_path: sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      tracker.bulk_upsert_file_metadata(
        [
          {
            path: first_path,
            dataset_type: "options",
            provider_name: "schwab",
            ticker: "SPXW",
            frequency: nil,
            expiration_date: "2026-04-17",
            row_count: 1,
            first_observed_at: "2026-04-10T14:35:00Z",
            last_observed_at: "2026-04-10T14:35:00Z",
            file_mtime: 1,
            file_size: 46
          },
          {
            path: second_path,
            dataset_type: "options",
            provider_name: "schwab",
            ticker: "SPXW",
            frequency: nil,
            expiration_date: "2026-04-17",
            row_count: 1,
            first_observed_at: "2026-04-10T14:30:00Z",
            last_observed_at: "2026-04-10T14:30:00Z",
            file_mtime: 1,
            file_size: 46
          },
          {
            path: third_path,
            dataset_type: "options",
            provider_name: "schwab",
            ticker: "SPXW",
            frequency: nil,
            expiration_date: "2026-04-17",
            row_count: 1,
            first_observed_at: "2026-04-11T14:30:00Z",
            last_observed_at: "2026-04-11T14:30:00Z",
            file_mtime: 1,
            file_size: 46
          }
        ]
      )
      loader = described_class.new(config: config, tracker: tracker)

      rows = loader.load_option_chains(
        provider: "schwab",
        ticker: "$SPX",
        expiration_date: Date.iso8601("2026-04-17"),
        start_date: Date.iso8601("2026-04-10"),
        end_date: Date.iso8601("2026-04-11"),
        order: :sample_time_asc,
        include_metadata: true
      ).to_a

      expect(rows.map { |row| row.fetch("description") }).to eq(%w[first second third])
      expect(rows.map { |row| row.fetch("metadata").fetch("sampled_at") }).to eq([
        Time.iso8601("2026-04-10T14:30:00Z"),
        Time.iso8601("2026-04-10T14:35:00Z"),
        Time.iso8601("2026-04-11T14:30:00Z")
      ])
    end
  end

  it "keeps the default option chain ordering unchanged" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      sqlite_path = File.join(dir, "tickrake.sqlite3")
      provider_dir = File.join(options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      earlier_path = File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-30-00.csv")
      later_path = File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-35-00.csv")
      File.write(earlier_path, "contract_type,symbol,description,expiration_date\nCALL,SPXW,first,2026-04-17\n")
      File.write(later_path, "contract_type,symbol,description,expiration_date\nCALL,SPXW,second,2026-04-17\n")

      config = build_config(history_dir: history_dir, options_dir: options_dir, sqlite_path: sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      tracker.bulk_upsert_file_metadata(
        [
          {
            path: earlier_path,
            dataset_type: "options",
            provider_name: "schwab",
            ticker: "SPXW",
            frequency: nil,
            expiration_date: "2026-04-17",
            row_count: 1,
            first_observed_at: "2026-04-10T14:30:00Z",
            last_observed_at: "2026-04-10T14:30:00Z",
            file_mtime: 1,
            file_size: 46
          },
          {
            path: later_path,
            dataset_type: "options",
            provider_name: "schwab",
            ticker: "SPXW",
            frequency: nil,
            expiration_date: "2026-04-17",
            row_count: 1,
            first_observed_at: "2026-04-10T14:35:00Z",
            last_observed_at: "2026-04-10T14:35:00Z",
            file_mtime: 1,
            file_size: 46
          }
        ]
      )
      loader = described_class.new(config: config, tracker: tracker)

      rows = loader.load_option_chains(
        provider: "schwab",
        ticker: "$SPX",
        expiration_date: Date.iso8601("2026-04-17"),
        start_date: Date.iso8601("2026-04-10"),
        end_date: Date.iso8601("2026-04-10")
      ).to_a

      expect(rows.map { |row| row.fetch("description") }).to eq(%w[first second])
    end
  end

  it "orders bucketed option chains by selected sample time when requested" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      sqlite_path = File.join(dir, "tickrake.sqlite3")
      provider_dir = File.join(options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      first_path = File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-34-00.csv")
      second_path = File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-30-00.csv")
      third_path = File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-39-00.csv")
      File.write(first_path, "contract_type,symbol,description,expiration_date\nCALL,SPXW,second,2026-04-17\n")
      File.write(second_path, "contract_type,symbol,description,expiration_date\nCALL,SPXW,first,2026-04-17\n")
      File.write(third_path, "contract_type,symbol,description,expiration_date\nCALL,SPXW,third,2026-04-17\n")

      config = build_config(history_dir: history_dir, options_dir: options_dir, sqlite_path: sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      tracker.bulk_upsert_file_metadata(
        [
          {
            path: first_path,
            dataset_type: "options",
            provider_name: "schwab",
            ticker: "SPXW",
            frequency: nil,
            expiration_date: "2026-04-17",
            row_count: 1,
            first_observed_at: "2026-04-10T14:34:00Z",
            last_observed_at: "2026-04-10T14:34:00Z",
            file_mtime: 1,
            file_size: 46
          },
          {
            path: second_path,
            dataset_type: "options",
            provider_name: "schwab",
            ticker: "SPXW",
            frequency: nil,
            expiration_date: "2026-04-17",
            row_count: 1,
            first_observed_at: "2026-04-10T14:30:00Z",
            last_observed_at: "2026-04-10T14:30:00Z",
            file_mtime: 1,
            file_size: 46
          },
          {
            path: third_path,
            dataset_type: "options",
            provider_name: "schwab",
            ticker: "SPXW",
            frequency: nil,
            expiration_date: "2026-04-17",
            row_count: 1,
            first_observed_at: "2026-04-10T14:39:00Z",
            last_observed_at: "2026-04-10T14:39:00Z",
            file_mtime: 1,
            file_size: 46
          }
        ]
      )
      loader = described_class.new(config: config, tracker: tracker)

      rows = loader.load_option_chains(
        provider: "schwab",
        ticker: "$SPX",
        expiration_date: Date.iso8601("2026-04-17"),
        start_date: Date.iso8601("2026-04-10"),
        end_date: Date.iso8601("2026-04-10"),
        frequency: "5min",
        order: :sample_time_asc,
        include_metadata: true
      ).to_a

      expect(rows.map { |row| row.fetch("description") }).to eq(%w[second third])
      expect(rows.map { |row| row.fetch("metadata").fetch("sampled_at") }).to eq([
        Time.iso8601("2026-04-10T14:34:00Z"),
        Time.iso8601("2026-04-10T14:39:00Z")
      ])
    end
  end

  it "rejects unsupported order values" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      sqlite_path = File.join(dir, "tickrake.sqlite3")
      config = build_config(history_dir: history_dir, options_dir: options_dir, sqlite_path: sqlite_path)
      loader = described_class.new(config: config, tracker: Tickrake::Tracker.new(config.sqlite_path))

      expect do
        loader.load_option_chains(
          provider: "schwab",
          ticker: "$SPX",
          start_date: Date.iso8601("2026-04-10"),
          end_date: Date.iso8601("2026-04-10"),
          order: :newest_first
        ).to_a
      end.to raise_error(Tickrake::Error, "Unsupported order: newest_first")
    end
  end
end
