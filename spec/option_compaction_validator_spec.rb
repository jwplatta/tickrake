# frozen_string_literal: true

RSpec.describe Tickrake::OptionCompactionValidator do
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

  it "validates a compacted csv against its source snapshot files" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      sample_dir = File.join(config.options_dir, "schwab", "2026", "06", "26")
      FileUtils.mkdir_p(sample_dir)
      File.write(
        File.join(sample_dir, "SPXW_exp2026-06-26_2026-06-26_14-30-00.csv"),
        <<~CSV
          contract_type,symbol,description,strike,expiration_date,mark,bid,bid_size,ask,ask_size,last,last_size,open_interest,total_volume,delta,gamma,theta,vega,rho,volatility,theoretical_volatility,theoretical_option_value,intrinsic_value,extrinsic_value,underlying_price
          CALL,SPXW1,desc1,2800.0,2026-06-26,1.1,1.0,2,1.2,3,1.15,1,10,20,0.5,0.1,-0.2,0.3,0.05,0.22,0.21,1.05,0.5,0.55,6000.0
        CSV
      )
      File.write(
        File.join(sample_dir, "SPXW_exp2026-06-27_2026-06-26_14-35-00.csv"),
        <<~CSV
          contract_type,symbol,description,strike,expiration_date,mark,bid,bid_size,ask,ask_size,last,last_size,open_interest,total_volume,delta,gamma,theta,vega,rho,volatility,theoretical_volatility,theoretical_option_value,intrinsic_value,extrinsic_value,underlying_price
          PUT,SPXW2,desc2,2805.0,2026-06-27,2.1,2.0,4,2.2,5,2.15,1,11,21,-0.5,0.2,-0.3,0.4,-0.05,0.32,0.31,2.05,0.6,1.45,6001.0
        CSV
      )
      compacted_path = File.join(sample_dir, "SPXW_samples_2026-06-26.csv")
      File.write(
        compacted_path,
        <<~CSV
          contract_type,symbol,description,strike,expiration_date,mark,bid,bid_size,ask,ask_size,last,last_size,open_interest,total_volume,delta,gamma,theta,vega,rho,volatility,theoretical_volatility,theoretical_option_value,intrinsic_value,extrinsic_value,underlying_price,sampled_at
          CALL,SPXW1,desc1,2800.0,2026-06-26,1.1,1.0,2,1.2,3,1.15,1,10,20,0.5,0.1,-0.2,0.3,0.05,0.22,0.21,1.05,0.5,0.55,6000.0,2026-06-26T14:30:00Z
          PUT,SPXW2,desc2,2805.0,2026-06-27,2.1,2.0,4,2.2,5,2.15,1,11,21,-0.5,0.2,-0.3,0.4,-0.05,0.32,0.31,2.05,0.6,1.45,6001.0,2026-06-26T14:35:00Z
        CSV
      )

      result = described_class.new(
        config: config,
        option_root: "SPXW",
        sample_date: Date.new(2026, 6, 26)
      ).validate

      expect(result.safe_to_delete).to eq(true)
      expect(result.source_paths.length).to eq(2)
      expect(result.compacted_path).to eq(compacted_path)
      expect(result.expected_row_count).to eq(2)
      expect(result.actual_row_count).to eq(2)
      expect(result.errors).to eq([])
    end
  end

  it "fails immediately when the compacted csv file does not exist" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)

      result = described_class.new(
        config: config,
        option_root: "SPXW",
        sample_date: Date.new(2026, 6, 26)
      ).validate

      expect(result.safe_to_delete).to eq(false)
      expect(result.errors).to include(a_string_including("Compacted CSV file not found"))
      expect(result.source_paths).to eq([])
      expect(result.actual_row_count).to eq(0)
    end
  end

  it "reports mismatches when the compacted csv differs from expected rows" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      sample_dir = File.join(config.options_dir, "schwab", "2026", "06", "26")
      FileUtils.mkdir_p(sample_dir)
      File.write(
        File.join(sample_dir, "SPXW_exp2026-06-26_2026-06-26_14-30-00.csv"),
        <<~CSV
          contract_type,symbol,description,strike,expiration_date,mark,bid,bid_size,ask,ask_size,last,last_size,open_interest,total_volume,delta,gamma,theta,vega,rho,volatility,theoretical_volatility,theoretical_option_value,intrinsic_value,extrinsic_value,underlying_price
          CALL,SPXW1,desc1,2800.0,2026-06-26,1.1,1.0,2,1.2,3,1.15,1,10,20,0.5,0.1,-0.2,0.3,0.05,0.22,0.21,1.05,0.5,0.55,6000.0
        CSV
      )
      compacted_path = File.join(sample_dir, "SPXW_samples_2026-06-26.csv")
      File.write(
        compacted_path,
        <<~CSV
          contract_type,symbol,description,strike,expiration_date,mark,bid,bid_size,ask,ask_size,last,last_size,open_interest,total_volume,delta,gamma,theta,vega,rho,volatility,theoretical_volatility,theoretical_option_value,intrinsic_value,extrinsic_value,underlying_price,sampled_at
          CALL,SPXW1,desc1,2800.0,2026-06-26,999.9,1.0,2,1.2,3,1.15,1,10,20,0.5,0.1,-0.2,0.3,0.05,0.22,0.21,1.05,0.5,0.55,6000.0,2026-06-26T14:30:00Z
        CSV
      )

      result = described_class.new(
        config: config,
        option_root: "SPXW",
        sample_date: Date.new(2026, 6, 26)
      ).validate

      expect(result.safe_to_delete).to eq(false)
      expect(result.errors).to include("First row mismatch at row 1.")
      expect(result.source_paths.length).to eq(1)
    end
  end
end
