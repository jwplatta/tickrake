# frozen_string_literal: true

module Tickrake
  class MaintenanceJob
    Result = Struct.new(:task, :processed_dates, :artifacts_written, keyword_init: true)

    def initialize(runtime, scheduled_job:, start_date: nil, end_date: nil, progress_reporter: nil)
      @runtime = runtime
      @scheduled_job = scheduled_job
      @start_date = start_date
      @end_date = end_date
      @progress_reporter = progress_reporter
    end

    def run(now: Time.now)
      @runtime.with_timezone do
        @runtime.logger.info("Starting maintenance job #{@scheduled_job.name} task=#{@scheduled_job.task} at #{now.utc.iso8601}")

        result = case @scheduled_job.task
                 when "compact_option_samples"
                   Tickrake::MaintenanceTasks::CompactOptionSamples.new(
                     runtime: @runtime,
                     scheduled_job: @scheduled_job,
                     start_date: @start_date,
                     end_date: @end_date,
                     progress_reporter: @progress_reporter
                   ).run(now: now)
                 else
                   raise Tickrake::Error, "Unknown maintenance task `#{@scheduled_job.task}`."
                 end

        @runtime.logger.info("Completed maintenance job #{@scheduled_job.name} task=#{@scheduled_job.task} at #{Time.now.utc.iso8601}")
        result
      end
    end
  end
end
