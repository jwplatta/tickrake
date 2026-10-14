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
      datastores: {},
      data_dir: File.join(dir, "data"),
      candles_dir: File.join(dir, "data", "candles"),
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

  it "supports writing only parquet without creating csv" do
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

      Dir.mktmpdir do |tmp_dir|
        parquet_path = File.join(tmp_dir, "duckdb.parquet")
        result = Tickrake::Storage::DuckdbOptionCompactedWriter.new.write(
          raw_files: raw_files,
          parquet_path: parquet_path,
          sampled_at_resolver: context.dataset.method(:sampled_at_for_path)
        )

        expect(result.csv_path).to be_nil
        expect(result.parquet_path).to eq(parquet_path)
        expect(File.exist?(parquet_path)).to eq(true)
        expect(result.row_count).to eq(2)
      end
    end
  end

  it "raises an error if neither csv_path nor parquet_path is provided" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      raw_files = write_raw_fixture(config)
      expect do
        Tickrake::Storage::DuckdbOptionCompactedWriter.new.write(
          raw_files: raw_files,
          sampled_at_resolver: ->(_) { Time.now }
        )
      end.to raise_error(Tickrake::Error, /requires at least one output path/)
    end
  end

  it "matches ruby and duckdb compaction when raw files contain quote_at, trade_at and new contract fields" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      sample_dir = File.join(config.options_dir, "schwab", "2027", "07", "01")
      FileUtils.mkdir_p(sample_dir)
      raw_a = File.join(sample_dir, "SPXW_exp2027-07-01_2027-07-01_14-30-00.csv")
      headers = Tickrake::Storage::OptionSampleWriter::CSV_HEADERS.join(",")
      row_a = "CALL,SPXW1,desc1,2800.0,2027-07-01,1.0,1.2,0.9,1.1,1.1,1.0,2,1.2,3,1.15,1,10,20,4,0.5,0.1,-0.2,0.3,0.05,0.22,0.21,1.05,0.5,0.55,6000.0,2027-07-01T14:29:55Z,2027-07-01T14:29:50Z,0.55,0.05,4.5,PM,EUROPEAN,WEEKLY,100,false"
      File.write(raw_a, "#{headers}\n#{row_a}\n")

      context = Tickrake::Maintenance::OptionSamples::Context.new(
        config: config,
        tracker: tracker,
        provider_name: "schwab",
        option_root: "SPXW",
        sample_date: Date.new(2027, 7, 1),
        logger: Logger.new(nil)
      )

      Dir.mktmpdir do |tmp_dir|
        built = context.dataset.build_rows(sample_date: context.sample_date, raw_files: [raw_a])
        ruby_result = Tickrake::Storage::OptionCompactedWriter.new.write(
          csv_path: File.join(tmp_dir, "ruby.csv"),
          parquet_path: File.join(tmp_dir, "ruby.parquet"),
          headers: built.fetch(:headers),
          rows: built.fetch(:rows)
        )

        duckdb_result = Tickrake::Storage::DuckdbOptionCompactedWriter.new.write(
          raw_files: [raw_a],
          csv_path: File.join(tmp_dir, "duckdb.csv"),
          parquet_path: File.join(tmp_dir, "duckdb.parquet"),
          sampled_at_resolver: context.dataset.method(:sampled_at_for_path)
        )

        expect(File.read(duckdb_result.csv_path)).to eq(File.read(ruby_result.csv_path))
        expect(parquet_rows(duckdb_result.parquet_path)).to eq(parquet_rows(ruby_result.parquet_path))
        parquet_data = parquet_rows(duckdb_result.parquet_path)
        expect(parquet_data.first[30]).to eq("2027-07-01T14:29:55Z") # quote_at
        expect(parquet_data.first[31]).to eq("2027-07-01T14:29:50Z") # trade_at
        expect(parquet_data.first[32]).to eq(0.55)                  # time_value
        expect(parquet_data.first[35]).to eq("PM")                   # settlement_type
        expect(parquet_data.first[38]).to eq(100)                    # multiplier
      end
    end
  end

  it "compacts mixed same-day legacy and expanded snapshots and validates successfully" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      sample_dir = File.join(config.options_dir, "schwab", "2026", "10", "14")
      FileUtils.mkdir_p(sample_dir)

      legacy_headers = Tickrake::Storage::OptionSampleWriter::CSV_HEADERS[0...30]
      expanded_headers = Tickrake::Storage::OptionSampleWriter::CSV_HEADERS

      raw_legacy = File.join(sample_dir, "SPXW_exp2026-10-14_2026-10-14_13-25-00.csv")
      row_legacy = "CALL,SPXW1,desc1,2800.0,2026-10-14,1.0,1.2,0.9,1.1,1.1,1.0,2,1.2,3,1.15,1,10,20,4,0.5,0.1,-0.2,0.3,0.05,0.22,0.21,1.05,0.5,0.55,6000.0"
      File.write(raw_legacy, "#{legacy_headers.join(",")}\n#{row_legacy}\n")

      raw_expanded = File.join(sample_dir, "SPXW_exp2026-10-14_2026-10-14_13-30-27.csv")
      row_expanded = "CALL,SPXW1,desc1,2800.0,2026-10-14,1.0,1.2,0.9,1.1,1.1,1.0,2,1.2,3,1.15,1,10,20,4,0.5,0.1,-0.2,0.3,0.05,0.22,0.21,1.05,0.5,0.55,6000.0,2026-10-14T13:30:25Z,2026-10-14T13:30:20Z,0.55,0.05,4.5,PM,EUROPEAN,WEEKLY,100,false"
      File.write(raw_expanded, "#{expanded_headers.join(",")}\n#{row_expanded}\n")

      context = Tickrake::Maintenance::OptionSamples::Context.new(
        config: config,
        tracker: tracker,
        provider_name: "schwab",
        option_root: "SPXW",
        sample_date: Date.new(2026, 10, 14),
        logger: Logger.new(nil)
      )

      compactor = Tickrake::Maintenance::OptionSamples::Compactor.new(context: context)
      compact_result = compactor.run(artifacts: %w[csv parquet])
      expect(compact_result.success).to eq(true)
      expect(compact_result.errors).to be_empty
      expect(compact_result.row_count).to eq(2)

      # Verify Parquet content
      parquet_data = parquet_rows(context.compacted_path("parquet"))
      expect(parquet_data.length).to eq(2)

      # First row from legacy snapshot: missing fields are null, quote_at derived from sampled_at
      legacy_row = parquet_data.find { |r| r[40] == "2026-10-14T13:25:00Z" }
      expect(legacy_row[30]).to eq("2026-10-14T13:25:00Z") # quote_at derived from sampled_at
      expect(legacy_row[31]).to be_nil                      # trade_at is null
      expect(legacy_row[32]).to be_nil                      # time_value is null
      expect(legacy_row[35]).to be_nil                      # settlement_type is null

      # Second row from expanded snapshot: new fields preserved
      expanded_row = parquet_data.find { |r| r[40] == "2026-10-14T13:30:27Z" }
      expect(expanded_row[30]).to eq("2026-10-14T13:30:25Z") # quote_at
      expect(expanded_row[31]).to eq("2026-10-14T13:30:20Z") # trade_at
      expect(expanded_row[32]).to eq(0.55)                  # time_value
      expect(expanded_row[35]).to eq("PM")                   # settlement_type
      expect(expanded_row[38]).to eq(100)                    # multiplier

      # Parquet and CSV outputs match ruby compactor build_rows parity
      built = context.dataset.build_rows(sample_date: context.sample_date)
      expect(built.fetch(:rows).length).to eq(2)
      expect(built.fetch(:rows).first[30]).to eq("2026-10-14T13:25:00Z") # legacy quote_at derived from sampled_at
      expect(built.fetch(:rows).first[35]).to be_nil # settlement_type nil

      # Validator passes
      validator = Tickrake::Maintenance::OptionSamples::Validator.new(context: context)
      val_result = validator.run
      expect(val_result.safe_to_delete).to eq(true)
      expect(val_result.errors).to be_empty
    end
  end

  it "rejects snapshot files with malformed or unsupported headers identifying the file" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      sample_dir = File.join(config.options_dir, "schwab", "2026", "10", "14")
      FileUtils.mkdir_p(sample_dir)

      raw_unsupported = File.join(sample_dir, "SPXW_exp2026-10-14_2026-10-14_13-25-00.csv")
      File.write(raw_unsupported, "contract_type,symbol,unsupported_column\nCALL,SPXW1,foo\n")

      context = Tickrake::Maintenance::OptionSamples::Context.new(
        config: config,
        tracker: tracker,
        provider_name: "schwab",
        option_root: "SPXW",
        sample_date: Date.new(2026, 10, 14),
        logger: Logger.new(nil)
      )

      expect do
        context.dataset.build_rows(sample_date: context.sample_date, raw_files: [raw_unsupported])
      end.to raise_error(Tickrake::Error, /Option snapshot SPXW_exp2026-10-14_2026-10-14_13-25-00\.csv contains unsupported headers: unsupported_column/)

      compactor = Tickrake::Maintenance::OptionSamples::Compactor.new(context: context)
      res = compactor.run(artifacts: %w[parquet])
      expect(res.success).to eq(false)
      expect(res.errors.first).to match(/Option snapshot SPXW_exp2026-10-14_2026-10-14_13-25-00\.csv contains unsupported headers: unsupported_column/)
      # Source snapshot preserved
      expect(File.exist?(raw_unsupported)).to eq(true)
    end
  end

  it "rejects snapshot files with duplicate headers identifying the file" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      sample_dir = File.join(config.options_dir, "schwab", "2026", "10", "14")
      FileUtils.mkdir_p(sample_dir)

      raw_dup = File.join(sample_dir, "SPXW_exp2026-10-14_2026-10-14_13-25-00.csv")
      File.write(raw_dup, "strike,strike,symbol\n1,2,SPXW\n")

      writer = Tickrake::Storage::DuckdbOptionCompactedWriter.new
      expect do
        writer.write(
          raw_files: [raw_dup],
          parquet_path: File.join(dir, "out.parquet"),
          sampled_at_resolver: ->(_) { Time.now }
        )
      end.to raise_error(Tickrake::Error, /Option snapshot SPXW_exp2026-10-14_2026-10-14_13-25-00\.csv contains duplicate headers: strike/)
    end
  end
end
