# frozen_string_literal: true

RSpec.describe Tickrake::DteResolver do
  it "maps configured buckets to nearest available expirations" do
    expiration_entries = [
      Tickrake::OptionExpirationEntry.new(expiration_date: "2026-04-10", days_to_expiration: 1),
      Tickrake::OptionExpirationEntry.new(expiration_date: "2026-04-17", days_to_expiration: 8),
      Tickrake::OptionExpirationEntry.new(expiration_date: "2026-05-09", days_to_expiration: 30)
    ]
    resolved = described_class.new(expiration_entries: expiration_entries, target_buckets: [1, 8, 30]).resolve

    expect(resolved.map(&:days_to_expiration)).to eq([1, 8, 30])
    expect(resolved.find { |row| row.days_to_expiration == 30 }.requested_buckets).to eq([30])
  end

  it "deduplicates expirations hit by multiple buckets" do
    expiration_entries = [
      Tickrake::OptionExpirationEntry.new(expiration_date: "2026-05-06", days_to_expiration: 27)
    ]
    resolved = described_class.new(expiration_entries: expiration_entries, target_buckets: [27, 28]).resolve

    expect(resolved.length).to eq(1)
    expect(resolved.first.requested_buckets).to match_array([27, 28])
  end
end
