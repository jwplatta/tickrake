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
      elsif job.windows.nil? || job.windows.empty?
        run_once(runtime, job)
      else
        case job.type
        when "options"
          Tickrake::OptionsMonitorRunner.new(runtime, scheduled_job: job).run
        when "candles"
          Tickrake::CandlesSchedulerRunner.new(runtime, scheduled_job: job, from_config_start: from_config_start).run
        when "maintenance"
          Tickrake::MaintenanceSchedulerRunner.new(runtime, scheduled_job: job).run
        when "order_book"
          Tickrake::OrderBookRunner.new(runtime, scheduled_job: job).run
        when "level_one"
          Tickrake::LevelOneRunner.new(runtime, scheduled_job: job).run
        when "metadata_sync"
          Tickrake::MetadataSyncSchedulerRunner.new(runtime, scheduled_job: job).run
        when "intraday_publish"
          Tickrake::IntradayPublisherSchedulerRunner.new(runtime, scheduled_job: job).run
        when "events_ingest"
          Tickrake::EventsIngestorRunner.new(runtime, scheduled_job: job).run
        when "reconciler"
          Tickrake::ReconcilerRunner.new(runtime, scheduled_job: job).run
        else
          raise Tickrake::Error, "Unknown job type `#{job.type}`."
        end
      end
    end

    def self.run_once(runtime, job)
      case job.type
      when "metadata_sync"
        job_instance = Tickrake::MetadataSyncJob.new(runtime, scheduled_job: job)
        loop do
          count = job_instance.run
          break if count.zero?
        end
        runtime.tracker.checkpoint!
      when "options"
        Tickrake::OptionsJob.new(runtime, scheduled_job: job).run
      when "candles"
        Tickrake::CandlesJob.new(runtime, scheduled_job: job).run
      when "maintenance"
        Tickrake::MaintenanceJob.new(runtime, scheduled_job: job).run
      when "intraday_publish"
        Tickrake::IntradayPublisherJob.new(runtime, scheduled_job: job).run
      when "events_ingest"
        Tickrake::EventsIngestorJob.new(runtime, scheduled_job: job).run
      when "reconciler"
        Tickrake::ReconcilerJob.new(runtime, scheduled_job: job).run
      when "order_book", "level_one"
        raise Tickrake::Error, "#{job.type} jobs require a schedule (streaming jobs cannot run once)."
      else
        raise Tickrake::Error, "Unknown job type `#{job.type}`."
      end
    end
    private_class_method :run_once
  end
end
