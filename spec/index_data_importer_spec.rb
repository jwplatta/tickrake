# frozen_string_literal: true

RSpec.describe "index data importer and members query" do
  def build_config(sqlite_path:)
    Tickrake::Config.new(
      timezone: "America/Chicago",
      sqlite_path: sqlite_path,
      providers: {
        "ibkr-paper" => Tickrake::ProviderDefinition.new(name: "ibkr-paper", adapter: "ibkr", settings: {}, symbol_map: {})
      },
      default_provider_name: "ibkr-paper",
      option_root_tickers: {},
      data_dir: Dir.mktmpdir,
      history_dir: Dir.mktmpdir,
      options_dir: Dir.mktmpdir,
      max_workers: 2,
      retry_count: 1,
      retry_delay_seconds: 1,
      option_fetch_timeout_seconds: 30,
      candle_fetch_timeout_seconds: 30,
      import_jobs: [],
      jobs: []
    )
  end

  it "imports canonical files idempotently and queries current canonical members only" do
    Dir.mktmpdir do |dir|
      memberships_path = File.join(dir, "market_index_memberships.csv")
      tickers_path = File.join(dir, "tickers.csv")
      alias_history_path = File.join(dir, "ticker_alias_history.csv")
      sqlite_path = File.join(dir, "tickrake.sqlite3")

      File.write(memberships_path, <<~CSV)
        index_code,canonical_ticker,start_date,end_date
        SP500,META,2013-12-23,
        SP500,COR,2001-08-30,
        SP500,BFH,2013-12-23,2020-06-22
        SP500,BF-B,1996-01-02,
        SP500,AABA,1999-12-08,2017-06-19
      CSV
      File.write(tickers_path, <<~CSV)
        canonical_ticker,security_name,gics_sector,gics_sub_industry,headquarters_location,cik,founded,status
        META,Meta Platforms,Communication Services,Interactive Media & Services,Menlo Park,1326801,2004,active
        COR,Cencora,Health Care,Health Care Distributors,Conshohocken,1140859,1985,active
        BFH,Bread Financial,Financials,Consumer Finance,Columbus,1121788,2001,delisted_or_acquired
        BF-B,Brown-Forman,Consumer Staples,Distillers & Vintners,Louisville,14693,1870,active
        AABA,Altaba,Financials,Multi-Sector Holdings,New York,1011006,1994,delisted_or_acquired
      CSV
      File.write(alias_history_path, <<~CSV)
        canonical_ticker,alias_ticker,start_date,end_date
        META,FB,2013-12-23,2022-06-09
        COR,ABC,2001-08-30,2023-08-30
        BFH,ADS,2013-12-23,2020-06-22
      CSV

      tracker = Tickrake::Tracker.new(sqlite_path)
      importer = Tickrake::IndexData::Importer.new(tracker: tracker)
      2.times do
        importer.import!(
          memberships_path: memberships_path,
          tickers_path: tickers_path,
          alias_history_path: alias_history_path
        )
      end

      expect(tracker.send(:db).get_first_value("SELECT COUNT(*) FROM tickers")).to eq(5)
      expect(tracker.send(:db).table_info("market_index_memberships").map { |row| row["name"] }).to include("ticker_id")
      expect(tracker.send(:db).table_info("market_index_memberships").map { |row| row["name"] }).not_to include("canonical_ticker")
      expect(tracker.send(:db).table_info("ticker_alias_history").map { |row| row["name"] }).not_to include("alias_status", "notes")
      expect(tracker.members_for_index(index_code: "SP500", as_of: "2017-01-01")).to eq(%w[AABA BF-B BFH COR META])
      expect(tracker.members_for_index(index_code: "SP500", as_of: "2024-01-01")).to eq(%w[BF-B COR META])

      config = build_config(sqlite_path: sqlite_path)
      stdout = StringIO.new
      Tickrake::Query::Engine.new(config: config, tracker: tracker, stdout: stdout).run(
        type: "members",
        index_code: "SP500",
        as_of: Date.new(2017, 1, 1),
        format: "json"
      )

      payload = JSON.parse(stdout.string)
      expect(payload).to eq(
        "type" => "members",
        "index" => "SP500",
        "as_of" => "2017-01-01",
        "count" => 5,
        "tickers" => %w[AABA BF-B BFH COR META]
      )
      expect(payload.fetch("tickers")).not_to include("FB", "ABC")
    end
  end
end
