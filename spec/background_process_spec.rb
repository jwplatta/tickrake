# frozen_string_literal: true

RSpec.describe Tickrake::BackgroundProcess do
  it "records provider and candle restart flags in job metadata" do
    registry = instance_double(Tickrake::JobRegistry)
    stdout = StringIO.new

    allow(registry).to receive(:status).with("candles").and_return({ name: "candles", state: "stopped" })
    allow(registry).to receive(:write)
    allow(FileUtils).to receive(:mkdir_p)
    log_device = instance_double(File, close: true)
    allow(File).to receive(:open).and_return(log_device)
    allow(Process).to receive(:spawn).and_return(1234)
    allow(Process).to receive(:detach).with(1234)
    allow(Time).to receive(:now).and_return(Time.utc(2026, 4, 14, 12, 0, 0))

    described_class.new(registry: registry, stdout: stdout).start(
      job_name: "candles",
      config_path: "/tmp/tickrake.yml",
      from_config_start: true,
      provider_name: "ib_paper"
    )

    expect(registry).to have_received(:write).with(
      "candles",
      hash_including(
        pid: 1234,
        config_path: "/tmp/tickrake.yml",
        provider_name: "ib_paper",
        from_config_start: true
      )
    )
  end
end
