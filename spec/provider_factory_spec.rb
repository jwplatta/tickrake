# frozen_string_literal: true

RSpec.describe Tickrake::ProviderFactory do
  it "builds the configured Schwab provider" do
    provider_definition = Tickrake::ProviderDefinition.new(name: "schwab_main", adapter: "schwab", settings: {})
    config = instance_double(Tickrake::Config)
    allow(config).to receive(:provider_definition).with("schwab_main").and_return(provider_definition)
    client = instance_double("client")
    client_factory = instance_double(Tickrake::ClientFactory, build: client)

    provider = described_class.new(config, provider_name: "schwab_main", client_factory: client_factory).build

    expect(provider).to be_a(Tickrake::Providers::Schwab)
    expect(provider.provider_name).to eq("schwab_main")
    expect(provider.adapter_name).to eq("schwab")
  end

  it "builds the configured IBKR provider" do
    provider_definition = Tickrake::ProviderDefinition.new(
      name: "ib_paper",
      adapter: "ibkr",
      settings: { "host" => "127.0.0.1" }
    )
    config = instance_double(Tickrake::Config)
    allow(config).to receive(:provider_definition).with("ib_paper").and_return(provider_definition)
    client_factory = instance_double(Tickrake::ClientFactory)

    provider = described_class.new(config, provider_name: "ib_paper", client_factory: client_factory).build

    expect(provider).to be_a(Tickrake::Providers::Ibkr)
    expect(provider.provider_name).to eq("ib_paper")
    expect(provider.adapter_name).to eq("ibkr")
  end

  it "rejects building import-only Massive providers" do
    provider_definition = Tickrake::ProviderDefinition.new(name: "massive", adapter: "massive", settings: {})
    config = instance_double(Tickrake::Config)
    allow(config).to receive(:provider_definition).with("massive").and_return(provider_definition)

    expect do
      described_class.new(config, provider_name: "massive", client_factory: instance_double(Tickrake::ClientFactory)).build
    end.to raise_error(Tickrake::ConfigError, /import-only/)
  end
end
