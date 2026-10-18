# frozen_string_literal: true

RSpec.describe Tickrake::LogRetention do
  it "prunes aged files from one rotated log family" do
    Dir.mktmpdir do |dir|
      current = Time.utc(2026, 6, 28, 12, 0, 0)
      log_path = File.join(dir, "logs", "index_options.log")
      FileUtils.mkdir_p(File.dirname(log_path))

      fresh = log_path
      stale_one = "#{log_path}.0"
      stale_two = "#{log_path}.1"
      unrelated = File.join(dir, "logs", "eod_candles.log")

      File.write(fresh, "fresh")
      File.write(stale_one, "stale")
      File.write(stale_two, "stale")
      File.write(unrelated, "other")

      File.utime(current - 60, current - 60, fresh)
      File.utime(current - (20 * 86_400), current - (20 * 86_400), stale_one)
      File.utime(current - (30 * 86_400), current - (30 * 86_400), stale_two)
      File.utime(current - (30 * 86_400), current - (30 * 86_400), unrelated)

      pruned = described_class.new(log_path: log_path, retention_days: 14, now: current).prune!

      expect(pruned).to match_array([stale_one, stale_two])
      expect(File.exist?(fresh)).to eq(true)
      expect(File.exist?(stale_one)).to eq(false)
      expect(File.exist?(stale_two)).to eq(false)
      expect(File.exist?(unrelated)).to eq(true)
    end
  end

  it "does nothing when retention is disabled" do
    Dir.mktmpdir do |dir|
      log_path = File.join(dir, "logs", "cli.log")
      FileUtils.mkdir_p(File.dirname(log_path))
      File.write(log_path, "cli")

      pruned = described_class.new(log_path: log_path, retention_days: 0).prune!

      expect(pruned).to eq([])
      expect(File.exist?(log_path)).to eq(true)
    end
  end
end
