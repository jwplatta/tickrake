# frozen_string_literal: true

RSpec.describe Tickrake::ForegroundProcess do
  it "delegates to JobRunner and does not write to JobRegistry" do
    stdout = StringIO.new
    config = instance_double(Tickrake::Config)
    job = instance_double(Tickrake::ScheduledJobConfig, name: "spx_options")
    runtime = instance_double(Tickrake::Runtime)

    allow(Tickrake::ConfigLoader).to receive(:load).with("/tmp/tickrake.yml").and_return(config)
    allow(config).to receive(:job).with("spx_options").and_return(job)
    allow(Tickrake::Runtime).to receive(:new).with(
      config: config,
      provider_name: nil,
      config_path: "/tmp/tickrake.yml",
      log_path: anything
    ).and_return(runtime)
    allow(Tickrake::JobRunner).to receive(:run)
    allow(Tickrake::JobRegistry).to receive(:new)

    described_class.new(stdout: stdout).start(
      job_name: "spx_options",
      config_path: "/tmp/tickrake.yml"
    )

    expect(Tickrake::JobRunner).to have_received(:run).with(
      runtime,
      job,
      from_config_start: false,
      restart: false
    )
    expect(Tickrake::JobRegistry).not_to have_received(:new)
  end

  it "passes restart: true to JobRunner when restart is requested" do
    stdout = StringIO.new
    config = instance_double(Tickrake::Config)
    job = instance_double(Tickrake::ScheduledJobConfig, name: "spx_options")
    runtime = instance_double(Tickrake::Runtime)

    allow(Tickrake::ConfigLoader).to receive(:load).with("/tmp/tickrake.yml").and_return(config)
    allow(config).to receive(:job).with("spx_options").and_return(job)
    allow(Tickrake::Runtime).to receive(:new).and_return(runtime)
    allow(Tickrake::JobRunner).to receive(:run)

    described_class.new(stdout: stdout).start(
      job_name: "spx_options",
      config_path: "/tmp/tickrake.yml",
      restart: true
    )

    expect(Tickrake::JobRunner).to have_received(:run).with(runtime, job, from_config_start: false, restart: true)
  end
end
