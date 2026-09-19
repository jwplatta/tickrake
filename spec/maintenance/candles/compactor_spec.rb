# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::Maintenance::Candles::Compactor do
  let(:tmpdir) { Dir.mktmpdir("candle_compact") }
  let(:candles_dir) { File.join(tmpdir, "candles") }
  let(:logger) { instance_double(Logger, info: nil, error: nil) }
  let(:config) do
    instance_double(Tickrake::Config, candles_dir: candles_dir).tap do |c|
      allow(c).to receive(:option_snapshot_filename_timezone).and_return("UTC")
    end
  end
  let(:context) do
    Tickrake::Maintenance::Candles::Context.new(
      config: config,
      provider_name: "schwab",
      frequency: "1min",
      symbol: "SPY",
      logger: logger
    )
  end

  subject(:compactor) { described_class.new(context: context) }

  before do
    FileUtils.mkdir_p(File.join(candles_dir, "schwab", "1min"))
  end

  after { FileUtils.rm_rf(tmpdir) }

  def write_csv(rows)
    csv_path = context.candle_csv_path
    CSV.open(csv_path, "w") do |csv|
      csv << %w[datetime open high low close volume]
      rows.each { |r| csv << r }
    end
  end

  describe "#run" do
    it "compacts CSV rows into parquet files grouped by year" do
      write_csv([
        ["2025-12-31T20:59:00Z", 470.0, 471.0, 469.0, 470.5, 50_000],
        ["2026-01-02T14:30:00Z", 450.0, 451.0, 449.5, 450.5, 100_000],
        ["2026-01-02T14:31:00Z", 450.5, 452.0, 450.0, 451.5, 80_000]
      ])

      result = compactor.run

      expect(result).to be_successful
      expect(result.row_count).to eq(3)
      expect(result.years_written).to contain_exactly(2025, 2026)

      parquet_2025 = context.compacted_path(2025)
      parquet_2026 = context.compacted_path(2026)
      expect(File.exist?(parquet_2025)).to be true
      expect(File.exist?(parquet_2026)).to be true

      rows_2025 = Tickrake::Storage::CandleParquetWriter.new.read(parquet_2025)
      expect(rows_2025.size).to eq(1)

      rows_2026 = Tickrake::Storage::CandleParquetWriter.new.read(parquet_2026)
      expect(rows_2026.size).to eq(2)
    end

    it "truncates CSV to headers-only after compaction" do
      write_csv([["2026-09-19T14:30:00Z", 450.0, 451.0, 449.5, 450.5, 100_000]])

      compactor.run

      csv_content = File.read(context.candle_csv_path)
      expect(csv_content.strip).to eq("datetime,open,high,low,close,volume")
    end

    it "merges new rows with existing parquet data" do
      writer = Tickrake::Storage::CandleParquetWriter.new
      parquet_path = context.compacted_path(2026)
      FileUtils.mkdir_p(File.dirname(parquet_path))
      writer.write(parquet_path, rows: [["2026-01-02T14:30:00Z", 450.0, 451.0, 449.5, 450.5, 100_000]])

      write_csv([["2026-01-02T14:31:00Z", 450.5, 452.0, 450.0, 451.5, 80_000]])

      result = compactor.run

      expect(result).to be_successful
      expect(result.row_count).to eq(2)

      rows = writer.read(parquet_path)
      expect(rows.size).to eq(2)
    end

    it "deduplicates by datetime (new rows win)" do
      writer = Tickrake::Storage::CandleParquetWriter.new
      parquet_path = context.compacted_path(2026)
      FileUtils.mkdir_p(File.dirname(parquet_path))
      writer.write(parquet_path, rows: [["2026-01-02T14:30:00Z", 999.0, 999.0, 999.0, 999.0, 1]])

      write_csv([["2026-01-02T14:30:00Z", 450.0, 451.0, 449.5, 450.5, 100_000]])

      compactor.run

      rows = writer.read(parquet_path)
      expect(rows.size).to eq(1)
      expect(rows[0][1]).to eq(450.0)
    end

    it "returns skipped result for empty CSV" do
      write_csv([])

      result = compactor.run

      expect(result).to be_successful
      expect(result).to be_skipped
      expect(result.row_count).to eq(0)
    end

    it "returns skipped result for non-existent CSV" do
      result = compactor.run

      expect(result).to be_successful
      expect(result).to be_skipped
    end
  end
end
