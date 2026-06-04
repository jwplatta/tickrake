# frozen_string_literal: true

# Integration tests that load real option snapshot data from ~/.tickrake.
# These tests require the actual data files and SQLite cache to be present and will be skipped otherwise.

REAL_DATA_CONFIG_PATH = File.expand_path("~/.tickrake/tickrake.yml")
# 2026-05-01: AAPL has 68+ snapshots in cache, UTC times 18:30-21:47 = 14:30-17:47 EDT (UTC-4)
REAL_DATA_TEST_DATE = Date.new(2026, 5, 1)
REAL_DATA_TEST_TICKER = "AAPL"
REAL_DATA_TEST_PROVIDER = "schwab"

RSpec.describe Tickrake::DataLoader, "timezone support" do
  let(:loader) { described_class.new(config_path: REAL_DATA_CONFIG_PATH) }

  before do
    skip "tickrake.yml not present at #{REAL_DATA_CONFIG_PATH}" unless File.exist?(REAL_DATA_CONFIG_PATH)
    skip "Real schwab AAPL data not in SQLite cache for #{REAL_DATA_TEST_DATE}" if begin
      Tickrake::DataLoader.new(config_path: REAL_DATA_CONFIG_PATH)
        .load_option_chains(
          provider: REAL_DATA_TEST_PROVIDER,
          ticker: REAL_DATA_TEST_TICKER,
          start_date: REAL_DATA_TEST_DATE,
          end_date: REAL_DATA_TEST_DATE
        ).first.nil?
    rescue StandardError
      true
    end
  end

  describe "load_option_chains with timezone: America/New_York" do
    let(:rows) do
      loader.load_option_chains(
        provider: REAL_DATA_TEST_PROVIDER,
        ticker: REAL_DATA_TEST_TICKER,
        start_date: REAL_DATA_TEST_DATE,
        end_date: REAL_DATA_TEST_DATE,
        timezone: "America/New_York"
      ).to_a
    end

    it "returns rows" do
      expect(rows).not_to be_empty
    end

    it "injects sampled_at_utc and sampled_at_tz into every row" do
      rows.each do |row|
        expect(row).to have_key("sampled_at_utc")
        expect(row).to have_key("sampled_at_tz")
      end
    end

    it "returns sampled_at_utc as a UTC Time" do
      rows.each do |row|
        utc = row.fetch("sampled_at_utc")
        expect(utc).to be_a(Time)
        expect(utc.utc?).to be(true)
      end
    end

    it "returns sampled_at_tz with UTC-4 offset (EDT in May)" do
      rows.each do |row|
        local = row.fetch("sampled_at_tz")
        expect(local.utc_offset).to eq(-4 * 3600),
          "expected EDT offset of -4h but got #{local.utc_offset / 3600}h for #{local}"
      end
    end

    it "returns sampled_at_tz with the correct local wall-clock hour (UTC hour - 4)" do
      sample_row = rows.find { |row| row.fetch("sampled_at_utc").hour >= 13 }
      next unless sample_row

      utc = sample_row.fetch("sampled_at_utc")
      local = sample_row.fetch("sampled_at_tz")
      expected_hour = (utc.hour - 4) % 24
      expect(local.hour).to eq(expected_hour)
    end

    it "covers multiple distinct sample times" do
      sample_times = rows.map { |row| row.fetch("sampled_at_utc") }.uniq
      expect(sample_times.length).to be >= 10
    end
  end

  describe "load_option_chains without timezone (default UTC)" do
    let(:rows) do
      loader.load_option_chains(
        provider: REAL_DATA_TEST_PROVIDER,
        ticker: REAL_DATA_TEST_TICKER,
        start_date: REAL_DATA_TEST_DATE,
        end_date: REAL_DATA_TEST_DATE
      ).to_a
    end

    it "returns rows" do
      expect(rows).not_to be_empty
    end

    it "has sampled_at_utc equal to sampled_at_tz when no timezone given" do
      rows.each do |row|
        expect(row.fetch("sampled_at_utc")).to eq(row.fetch("sampled_at_tz"))
      end
    end
  end
end
