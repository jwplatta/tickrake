# frozen_string_literal: true

RSpec.describe Tickrake::JobRunner do
  let(:runtime) { instance_double(Tickrake::Runtime) }

  describe ".run with restart: false" do
    it "runs OptionsMonitorRunner for options jobs" do
      job = instance_double(Tickrake::ScheduledJobConfig, type: "options")
      runner = instance_double(Tickrake::OptionsMonitorRunner, run: nil)
      allow(Tickrake::OptionsMonitorRunner).to receive(:new).with(runtime, scheduled_job: job).and_return(runner)

      described_class.run(runtime, job, from_config_start: false, restart: false)

      expect(runner).to have_received(:run)
    end

    it "runs CandlesSchedulerRunner for candles jobs" do
      job = instance_double(Tickrake::ScheduledJobConfig, type: "candles")
      runner = instance_double(Tickrake::CandlesSchedulerRunner, run: nil)
      allow(Tickrake::CandlesSchedulerRunner).to receive(:new).with(runtime, scheduled_job: job, from_config_start: true).and_return(runner)

      described_class.run(runtime, job, from_config_start: true, restart: false)

      expect(runner).to have_received(:run)
    end

    it "runs MaintenanceSchedulerRunner for maintenance jobs" do
      job = instance_double(Tickrake::ScheduledJobConfig, type: "maintenance")
      runner = instance_double(Tickrake::MaintenanceSchedulerRunner, run: nil)
      allow(Tickrake::MaintenanceSchedulerRunner).to receive(:new).with(runtime, scheduled_job: job).and_return(runner)

      described_class.run(runtime, job, from_config_start: false, restart: false)

      expect(runner).to have_received(:run)
    end

    it "raises for unknown job types" do
      job = instance_double(Tickrake::ScheduledJobConfig, type: "unknown")

      expect do
        described_class.run(runtime, job, from_config_start: false, restart: false)
      end.to raise_error(Tickrake::Error, /Unknown job type/)
    end
  end

  describe ".run with restart: true" do
    it "runs SchedulerSupervisor regardless of job type" do
      job = instance_double(Tickrake::ScheduledJobConfig, type: "options")
      supervisor = instance_double(Tickrake::SchedulerSupervisor, run: nil)
      allow(Tickrake::SchedulerSupervisor).to receive(:new).with(runtime, scheduled_job: job, from_config_start: false).and_return(supervisor)

      described_class.run(runtime, job, from_config_start: false, restart: true)

      expect(supervisor).to have_received(:run)
    end
  end
end
