# frozen_string_literal: true

RSpec.describe Tickrake::PathSupport do
  it "stores logs in a dedicated logs directory under the tickrake home" do
    allow(described_class).to receive(:home_dir).and_return("/tmp/tickrake-home")

    expect(described_class.logs_dir).to eq("/tmp/tickrake-home/logs")
    expect(described_class.cli_log_path).to eq("/tmp/tickrake-home/logs/cli.log")
    expect(described_class.named_log_path("index_options")).to eq("/tmp/tickrake-home/logs/index_options.log")
  end
end
