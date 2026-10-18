# frozen_string_literal: true

RSpec.describe Tickrake::Storage::CandleReconciler do
  it "deduplicates overlapping bars and sorts them by timestamp" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "SPY_day.csv")
      File.write(path, <<~CSV)
        datetime,open,high,low,close,volume
        2026-04-01T21:00:00Z,1.0,2.0,0.5,1.5,10
      CSV

      bars = [
        Tickrake::Data::Bar.new(datetime: Time.utc(2026, 4, 2, 21, 0, 0), open: 2.0, high: 3.0, low: 1.5, close: 2.5, volume: 20),
        Tickrake::Data::Bar.new(datetime: Time.utc(2026, 4, 1, 21, 0, 0), open: 9.0, high: 9.0, low: 9.0, close: 9.0, volume: 99)
      ]

      described_class.new.write(path: path, bars: bars)

      rows = CSV.read(path, headers: true)
      expect(rows.map { |row| row.fetch("datetime") }).to eq([
        "2026-04-01T21:00:00Z",
        "2026-04-02T21:00:00Z"
      ])
      expect(rows.first.fetch("open")).to eq("9.0")
    end
  end
end
