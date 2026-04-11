# frozen_string_literal: true

RSpec.describe Tickrake::ProviderFactory do
  it "builds the configured Schwab provider" do
    config = instance_double(Tickrake::Config, provider: "schwab")
    client = instance_double("client")
    client_factory = instance_double(Tickrake::ClientFactory, build: client)

    provider = described_class.new(config, client_factory: client_factory).build

    expect(provider).to be_a(Tickrake::Providers::Schwab)
  end
end
