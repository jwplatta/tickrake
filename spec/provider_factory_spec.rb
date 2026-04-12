# frozen_string_literal: true

RSpec.describe Tickrake::ProviderFactory do
  it "builds the configured Schwab provider" do
    config = instance_double(Tickrake::Config, provider: "schwab", provider_settings: {})
    client = instance_double("client")
    client_factory = instance_double(Tickrake::ClientFactory, build: client)

    provider = described_class.new(config, client_factory: client_factory).build

    expect(provider).to be_a(Tickrake::Providers::Schwab)
  end

  it "builds the configured IBKR provider" do
    config = instance_double(Tickrake::Config, provider: "ibkr", provider_settings: { "host" => "127.0.0.1" })
    client_factory = instance_double(Tickrake::ClientFactory)

    provider = described_class.new(config, client_factory: client_factory).build

    expect(provider).to be_a(Tickrake::Providers::Ibkr)
  end
end
