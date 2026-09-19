# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::Storage::CandleParquetWriter do
  let(:tmpdir) { Dir.mktmpdir("candle_parquet") }
  let(:path) { File.join(tmpdir, "test.parquet") }

  subject(:writer) { described_class.new }

  after { FileUtils.rm_rf(tmpdir) }

  describe "#write and #read" do
    it "writes rows and reads them back" do
      rows = [
        ["2026-09-19T14:30:00Z", 450.0, 451.0, 449.5, 450.5, 100_000],
        ["2026-09-19T14:31:00Z", 450.5, 452.0, 450.0, 451.5, 80_000]
      ]

      writer.write(path, rows: rows)
      expect(File.exist?(path)).to be true

      read_back = writer.read(path)
      expect(read_back.size).to eq(2)
      expect(read_back[0][0]).to eq("2026-09-19T14:30:00Z")
      expect(read_back[0][1]).to eq(450.0)
      expect(read_back[0][5]).to eq(100_000)
      expect(read_back[1][0]).to eq("2026-09-19T14:31:00Z")
    end

    it "creates parent directories" do
      nested_path = File.join(tmpdir, "a", "b", "c.parquet")
      writer.write(nested_path, rows: [["2026-01-01T00:00:00Z", 1.0, 2.0, 0.5, 1.5, 100]])
      expect(File.exist?(nested_path)).to be true
    end

    it "does not leave tmp files after write" do
      writer.write(path, rows: [["2026-01-01T00:00:00Z", 1.0, 2.0, 0.5, 1.5, 100]])
      expect(Dir.glob(File.join(tmpdir, "*.tmp"))).to be_empty
    end
  end

  describe "#read" do
    it "returns empty array for non-existent file" do
      expect(writer.read("/nonexistent/path.parquet")).to eq([])
    end
  end
end
