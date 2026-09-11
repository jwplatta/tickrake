# frozen_string_literal: true

require "spec_helper"
require "tmpdir"

RSpec.describe Tickrake::EventsWriter do
  let(:tmpdir) { Dir.mktmpdir }
  let(:logger) { instance_double(Logger, info: nil, warn: nil) }
  let(:job_name) { "test_job" }
  let(:rotation_interval) { 60 }

  subject(:writer) do
    described_class.new(
      pending_events_dir: tmpdir,
      job_name: job_name,
      rotation_interval_seconds: rotation_interval,
      logger: logger
    )
  end

  after { FileUtils.rm_rf(tmpdir) }

  describe "#write" do
    it "creates a .ndjson.tmp file and writes an event as JSON" do
      writer.write("symbol" => "SPY", "bid" => 1.23)

      tmp_files = Dir.glob(File.join(tmpdir, "*.ndjson.tmp"))
      expect(tmp_files.length).to eq(1)
      line = JSON.parse(File.read(tmp_files.first).strip)
      expect(line["symbol"]).to eq("SPY")
      expect(line["bid"]).to eq(1.23)
    end

    it "appends multiple events to the same file within rotation interval" do
      writer.write("seq" => 1)
      writer.write("seq" => 2)

      tmp_files = Dir.glob(File.join(tmpdir, "*.ndjson.tmp"))
      expect(tmp_files.length).to eq(1)
      lines = File.readlines(tmp_files.first).map { |l| JSON.parse(l) }
      expect(lines.map { |l| l["seq"] }).to eq([1, 2])
    end

    it "rotates to .ndjson when rotation interval elapses" do
      writer.write("seq" => 1)

      # Force rotation by backdating @current_opened_at
      writer.instance_variable_set(:@current_opened_at, Time.now - (rotation_interval + 1))

      writer.write("seq" => 2)

      ndjson_files = Dir.glob(File.join(tmpdir, "*.ndjson"))
      tmp_files    = Dir.glob(File.join(tmpdir, "*.ndjson.tmp"))
      expect(ndjson_files.length).to eq(1)
      expect(tmp_files.length).to eq(1)
    end
  end

  describe "#close" do
    it "renames the open .tmp file to .ndjson" do
      writer.write("x" => 1)
      expect(Dir.glob(File.join(tmpdir, "*.ndjson.tmp")).length).to eq(1)

      writer.close

      expect(Dir.glob(File.join(tmpdir, "*.ndjson.tmp"))).to be_empty
      expect(Dir.glob(File.join(tmpdir, "*.ndjson")).length).to eq(1)
    end

    it "is a no-op when no file is open" do
      expect { writer.close }.not_to raise_error
    end
  end

  describe "#recover_stale_files" do
    it "renames .ndjson.tmp files older than 2x rotation interval" do
      stale_path = File.join(tmpdir, "#{job_name}_20260101T000000Z.ndjson.tmp")
      File.write(stale_path, "{}\n")
      FileUtils.touch(stale_path, mtime: Time.now - (rotation_interval * 2 + 1))

      writer.recover_stale_files

      expect(File.exist?(stale_path)).to be(false)
      expect(File.exist?(stale_path.sub(/\.tmp$/, ""))).to be(true)
    end

    it "ignores fresh .ndjson.tmp files" do
      fresh_path = File.join(tmpdir, "#{job_name}_20260101T000000Z.ndjson.tmp")
      File.write(fresh_path, "{}\n")

      writer.recover_stale_files

      expect(File.exist?(fresh_path)).to be(true)
    end
  end
end
