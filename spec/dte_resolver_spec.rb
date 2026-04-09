# frozen_string_literal: true

RSpec.describe Tickrake::DteResolver do
  let(:chain) do
    SchwabRb::DataObjects::OptionExpirationChain.build(
      JSON.parse(File.read(File.expand_path("fixtures/option_expiration_chain.json", __dir__)))
    )
  end

  it "maps configured buckets to nearest available expirations" do
    resolved = described_class.new(expiration_chain: chain, target_buckets: [1, 8, 30]).resolve

    expect(resolved.map(&:days_to_expiration)).to eq([1, 8, 30])
    expect(resolved.find { |row| row.days_to_expiration == 30 }.requested_buckets).to eq([30])
  end

  it "deduplicates expirations hit by multiple buckets" do
    resolved = described_class.new(expiration_chain: chain, target_buckets: [27, 28]).resolve

    expect(resolved.length).to eq(1)
    expect(resolved.first.requested_buckets).to match_array([27, 28])
  end
end
