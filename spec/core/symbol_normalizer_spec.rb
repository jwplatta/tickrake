# frozen_string_literal: true

RSpec.describe Tickrake::Core::SymbolNormalizer do
  subject(:normalizer) { described_class.new }

  let(:provider_definition) do
    Tickrake::ProviderDefinition.new(
      name: "schwab",
      adapter: "schwab",
      settings: {},
      symbol_map: { "/ES" => "^ES", "/NQ" => "^NQ", "/RTY" => "^RTY" }
    )
  end

  it "normalizes symbols for canonical comparison and storage tokens" do
    expect(normalizer.canonical("$spx")).to eq("SPX")
    expect(normalizer.storage_token("$spx")).to eq("SPX")
    expect(normalizer.same_symbol?("$SPX", "spx")).to eq(true)
    expect(normalizer.canonical("/es", provider_definition: provider_definition)).to eq("^ES")
    expect(normalizer.storage_token("/es", provider_definition: provider_definition)).to eq("^ES")
    expect(normalizer.same_symbol?("/ES", "^es", provider_definition: provider_definition)).to eq(true)
  end
end
