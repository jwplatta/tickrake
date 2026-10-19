# frozen_string_literal: true

RSpec.describe Tickrake::Core::FrequencyNormalizer do
  subject(:normalizer) { described_class.new }

  it "normalizes minute aliases" do
    expect(normalizer.normalize("1min")).to eq("1min")
    expect(normalizer.normalize("1m")).to eq("1min")
    expect(normalizer.normalize("minute")).to eq("1min")
    expect(normalizer.normalize("5min")).to eq("5min")
    expect(normalizer.normalize("5m")).to eq("5min")
  end

  it "normalizes daily aliases" do
    expect(normalizer.normalize("day")).to eq("day")
    expect(normalizer.normalize("daily")).to eq("day")
    expect(normalizer.normalize("1d")).to eq("day")
  end

  it "raises for unknown frequencies" do
    expect { normalizer.normalize("invalid") }.to raise_error(Tickrake::Error, /Unknown candle frequency/)
  end
end
