# frozen_string_literal: true

RSpec.describe "duckdb option compaction parity" do
  def build_config(dir)
    Tickrake::Config.new(
      timezone: "America/Chicago",
      sqlite_path: File.join(dir, "tickrake.sqlite3"),
      providers: {
        "schwab" => Tickrake::ProviderDefinition.new(name: "schwab", adapter: "schwab", settings: {}, symbol_map: {})
      },
      default_provider_name: "schwab",
      option_root_tickers: { "SPXW" => "SPX" },
      option_snapshot_filename_timezone: "utc",
      archives: {},
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

  def write_raw_fixture(config)
    sample_dir = File.join(config.options_dir, "schwab", "2027", "07", "01")
    FileUtils.mkdir_p(sample_dir)
    raw_a = File.join(sample_dir, "SPXW_exp2027-07-01_2027-07-01_14-30-00.csv")
    raw_b = File.join(sample_dir, "SPXW_exp2027-07-02_2027-07-01_14-35-00.csv")
    File.write(raw_a, <<~CSV)
      contract_type,symbol,description,strike,expiration_date,open,high,low,close,mark,bid,bid_size,ask,ask_size,last,last_size,open_interest,total_volume,transactions,delta,gamma,theta,vega,rho,volatility,theoretical_volatility,theoretical_option_value,intrinsic_value,extrinsic_value,underlying_price
      CALL,SPXW1,desc1,2800.0,2027-07-01,1.0,1.2,0.9,1.1,1.1,1.0,2,1.2,3,1.15,1,10,20,4,0.5,0.1,-0.2,0.3,0.05,0.22,0.21,1.05,0.5,0.55,6000.0
    CSV
    File.write(raw_b, <<~CSV)
      contract_type,symbol,description,strike,expiration_date,open,high,low,close,mark,bid,bid_size,ask,ask_size,last,last_size,open_interest,total_volume,transactions,delta,gamma,theta,vega,rho,volatility,theoretical_volatility,theoretical_option_value,intrinsic_value,extrinsic_value,underlying_price
      PUT,SPXW2,desc2,2805.0,2027-07-02,2.0,2.2,1.9,2.1,2.1,2.0,4,2.2,5,2.15,1,11,21,6,-0.5,0.2,-0.3,0.4,-0.05,0.32,0.31,2.05,0.6,1.45,6001.0
    CSV
    [raw_a, raw_b]
  end

  def parquet_rows(path)
    DuckDB::Database.open do |db|
      db.connect do |con|
        con.query("SELECT * FROM read_parquet('#{path.gsub("'", "''")}') ORDER BY sampled_at, expiration_date, contract_type, strike, symbol").map do |row|
          row.map do |value|
            case value
            when Time
              value.utc.iso8601
            when Date
              value.iso8601
            else
              value
            end
          end
        end
      end
    end
  end

  it "matches the ruby compactor outputs for csv content and parquet rows" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      raw_files = write_raw_fixture(config)
      context = Tickrake::Maintenance::OptionSamples::Context.new(
        config: config,
        tracker: tracker,
        provider_name: "schwab",
        option_root: "SPXW",
        sample_date: Date.new(2027, 7, 1),
        logger: Logger.new(nil)
      )

      ruby_result = nil
      duckdb_result = nil

      Dir.mktmpdir do |tmp_dir|
        built = context.dataset.build_rows(sample_date: context.sample_date, raw_files: raw_files)
        ruby_result = Tickrake::Storage::OptionCompactedWriter.new.write(
          csv_path: File.join(tmp_dir, "ruby.csv"),
          parquet_path: File.join(tmp_dir, "ruby.parquet"),
          headers: built.fetch(:headers),
          rows: built.fetch(:rows)
        )

        duckdb_result = Tickrake::Storage::DuckdbOptionCompactedWriter.new.write(
          raw_files: raw_files,
          csv_path: File.join(tmp_dir, "duckdb.csv"),
          parquet_path: File.join(tmp_dir, "duckdb.parquet"),
          sampled_at_resolver: context.dataset.method(:sampled_at_for_path)
        )

        expect(File.read(duckdb_result.csv_path)).to eq(File.read(ruby_result.csv_path))
        expect(parquet_rows(duckdb_result.parquet_path)).to eq(parquet_rows(ruby_result.parquet_path))
        expect(duckdb_result.row_count).to eq(built.fetch(:rows).length)
        expect(duckdb_result.first_sampled_at.utc.iso8601).to eq("2027-07-01T14:30:00Z")
        expect(duckdb_result.last_sampled_at.utc.iso8601).to eq("2027-07-01T14:35:00Z")
      end
    end
  end
end
