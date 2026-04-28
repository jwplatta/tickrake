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
      instance_double(Tickrake::Config, job: instance_double(Tickrake::ScheduledJobConfig, name: "index_options", manual?: false))
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

  it "starts all scheduled jobs without starting manual jobs" do
    registry = instance_double(Tickrake::JobRegistry)
    starter = instance_double(Tickrake::BackgroundProcess)
    stdout = StringIO.new
    controller = described_class.new(registry: registry, starter: starter, stdout: stdout)
    scheduled_job = instance_double(Tickrake::ScheduledJobConfig, name: "index_options", scheduled?: true)
    manual_job = instance_double(Tickrake::ScheduledJobConfig, name: "manual_options", scheduled?: false)

    allow(Tickrake::ConfigLoader).to receive(:load).with("/tmp/tickrake.yml").and_return(
      instance_double(Tickrake::Config, jobs: [scheduled_job, manual_job])
    )
    allow(starter).to receive(:start)

    controller.start(target: "all", config_path: "/tmp/tickrake.yml")

    expect(starter).to have_received(:start).with(
      job_name: "index_options",
      config_path: "/tmp/tickrake.yml",
      provider_name: nil,
      from_config_start: false,
      restart: false
    )
    expect(starter).not_to have_received(:start).with(hash_including(job_name: "manual_options"))
  end

  it "rejects explicit background control for manual jobs" do
    registry = instance_double(Tickrake::JobRegistry)
    starter = instance_double(Tickrake::BackgroundProcess)
    stdout = StringIO.new
    controller = described_class.new(registry: registry, starter: starter, stdout: stdout)
    manual_job = instance_double(Tickrake::ScheduledJobConfig, name: "manual_options", manual?: true)

    allow(Tickrake::ConfigLoader).to receive(:load).with("/tmp/tickrake.yml").and_return(
      instance_double(Tickrake::Config, job: manual_job)
    )

    expect do
      controller.start(target: "manual_options", config_path: "/tmp/tickrake.yml")
    end.to raise_error(Tickrake::Error, /Manual job `manual_options` cannot be controlled/)
  end
end
