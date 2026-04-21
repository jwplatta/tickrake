# frozen_string_literal: true

RSpec.describe Tickrake::JobControl do
  it "preserves stored restart policy when restarting a job without an explicit override" do
    registry = instance_double(Tickrake::JobRegistry)
    starter = instance_double(Tickrake::BackgroundProcess)
    stdout = StringIO.new
    controller = described_class.new(registry: registry, starter: starter, stdout: stdout)

    allow(registry).to receive(:read).with("index_options").and_return(
      config_path: "/tmp/custom.yml",
      provider_name: "schwab_live",
      from_config_start: true,
      restart: true
    )
    allow(registry).to receive(:status).with("index_options").and_return(name: "index_options", state: "stopped")
    allow(starter).to receive(:start)
    allow(Tickrake::ConfigLoader).to receive(:load).with(Tickrake::PathSupport.config_path).and_return(
      instance_double(Tickrake::Config, job: instance_double(Tickrake::ScheduledJobConfig, name: "index_options"))
    )

    controller.restart(target: "index_options")

    expect(starter).to have_received(:start).with(
      job_name: "index_options",
      config_path: "/tmp/custom.yml",
      provider_name: "schwab_live",
      from_config_start: true,
      restart: true
    )
  end
end
