# frozen_string_literal: true

RSpec.describe Tickrake::IndexData::Sp500CanonicalDataset do
  let(:memberships_source) { File.expand_path("../data/sp500_ticker_start_end.csv", __dir__) }
  let(:tickers_source) { File.expand_path("../data/sp500.csv", __dir__) }
  let(:status_source) { File.expand_path("../data/sp500_ticker_status_2015_2026.csv", __dir__) }

  it "builds canonical derivative files from the repo-owned S&P 500 sources" do
    Dir.mktmpdir do |dir|
      dataset = described_class.new(
        memberships_source: memberships_source,
        tickers_source: tickers_source,
        status_source: status_source,
        output_dir: dir
      )

      result = dataset.generate!

      expect(File.exist?(File.join(dir, "market_index_memberships.csv"))).to eq(true)
      expect(File.exist?(File.join(dir, "tickers.csv"))).to eq(true)
      expect(File.exist?(File.join(dir, "ticker_alias_history.csv"))).to eq(true)

      expect(result[:memberships]).to include(
        a_hash_including("canonical_ticker" => "META", "start_date" => "2013-12-23", "end_date" => nil),
        a_hash_including("canonical_ticker" => "COR", "start_date" => "2001-08-30", "end_date" => nil),
        a_hash_including("canonical_ticker" => "BFH", "start_date" => "2013-12-23", "end_date" => "2020-06-22")
      )

      expect(result[:memberships]).to include(
        a_hash_including("canonical_ticker" => "BF-B", "start_date" => "1996-01-02"),
        a_hash_including("canonical_ticker" => "BRK-B", "start_date" => "2010-02-16")
      )
      expect(result[:memberships].any? { |row| row["canonical_ticker"] == "FB" }).to eq(false)
      expect(result[:memberships].any? { |row| row["canonical_ticker"] == "ABC" }).to eq(false)

      expect(result[:tickers]).to include(
        a_hash_including("canonical_ticker" => "META", "security_name" => "Meta Platforms", "status" => "active"),
        a_hash_including("canonical_ticker" => "COR", "security_name" => "Cencora", "status" => "active")
      )

      expect(result[:alias_history]).to include(
        a_hash_including("canonical_ticker" => "META", "alias_ticker" => "FB"),
        a_hash_including("canonical_ticker" => "COR", "alias_ticker" => "ABC"),
        a_hash_including("canonical_ticker" => "BFH", "alias_ticker" => "ADS")
      )
      expect(result[:alias_history].any? { |row| row["alias_ticker"] == "BF-B" }).to eq(false)
      expect(result[:alias_history].any? { |row| row["alias_ticker"] == "BRK-B" }).to eq(false)
      expect(result[:memberships]).to include(a_hash_including("canonical_ticker" => "AABA", "end_date" => "2017-06-19"))
    end
  end

  it "fails on rename cycles in the status map" do
    Dir.mktmpdir do |dir|
      memberships_path = File.join(dir, "memberships.csv")
      tickers_path = File.join(dir, "tickers.csv")
      status_path = File.join(dir, "status.csv")

      File.write(memberships_path, "ticker,start_date,end_date\nAAA,2020-01-01,\n")
      File.write(tickers_path, "Symbol,Security,GICS Sector,GICS Sub-Industry,Headquarters Location,Date added,CIK,Founded\nAAA,AAA Corp,Tech,Software,Austin,2020-01-01,1,2020\n")
      File.write(status_path, <<~CSV)
        ticker,first_start,last_end,status,new_ticker
        AAA,2020-01-01,,renamed,BBB
        BBB,2020-01-01,,renamed,AAA
      CSV

      dataset = described_class.new(
        memberships_source: memberships_path,
        tickers_source: tickers_path,
        status_source: status_path,
        output_dir: dir
      )

      expect { dataset.generate! }.to raise_error(Tickrake::Error, /Rename cycle detected/)
    end
  end
end
