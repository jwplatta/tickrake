# frozen_string_literal: true

module Tickrake
  module JobRunner
    def self.run(runtime, job, from_config_start:, restart:)
      if restart
        Tickrake::SchedulerSupervisor.new(
          runtime,
          scheduled_job: job,
          from_config_start: from_config_start
        ).run
      else
        case job.type
        when "options"
          Tickrake::OptionsMonitorRunner.new(runtime, scheduled_job: job).run
        when "candles"
          Tickrake::CandlesSchedulerRunner.new(runtime, scheduled_job: job, from_config_start: from_config_start).run
        when "maintenance"
          Tickrake::MaintenanceSchedulerRunner.new(runtime, scheduled_job: job).run
        else
          raise Tickrake::Error, "Unknown job type `#{job.type}`."
        end
      end
    end
  end
end
