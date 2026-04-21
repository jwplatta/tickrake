# frozen_string_literal: true

RSpec.describe Tickrake::SchedulerSupervisor do
  let(:config) do
    Tickrake::ConfigLoader.load(File.expand_path("../config/tickrake.example.yml", __dir__))
  end
  let(:job) { config.job("index_options") }
  let(:logger) do
    instance_double(Logger, info: nil, error: nil).tap do |double|
      allow(double).to receive(:level=)
    end
  end
  let(:runtime) { Tickrake::Runtime.new(config: config, logger: logger, provider_name: "schwab", config_path: "/tmp/custom.yml") }
  let(:sleeper) do
    double("sleeper").tap do |double|
      allow(double).to receive(:sleep)
    end
  end

  it "restarts the scheduler after an unexpected exit with backoff" do
    status_fail = instance_double(Process::Status, success?: false, exitstatus: 1)
    status_ok = instance_double(Process::Status, success?: true, exitstatus: 0)

    allow(Process).to receive(:spawn).and_return(111, 222)
    allow(Process).to receive(:wait2).with(111).and_return([111, status_fail])
    allow(Process).to receive(:wait2).with(222).and_return([222, status_ok])

    described_class.new(runtime, scheduled_job: job, sleeper: sleeper).run

    expect(Process).to have_received(:spawn).twice
    expect(sleeper).to have_received(:sleep).with(described_class::RESTART_DELAY_SECONDS)
    expect(logger).to have_received(:error).with(/exited unexpectedly/)
  end
end
