# frozen_string_literal: true

module Tickrake
  module MaintenanceTasks
    class CompactOptionSamples
      Result = Struct.new(:task, :processed_dates, :artifacts_written, keyword_init: true)

      def initialize(runtime:, scheduled_job:, start_date: nil, end_date: nil, progress_reporter: nil)
        @runtime = runtime
        @scheduled_job = scheduled_job
        @start_date = start_date
        @end_date = end_date
        @progress_reporter = progress_reporter
      end

      def run(now: Time.now)
        task = Tickrake::MaintenanceStepConfig.new(
          action: "compact",
          subject: "option_samples",
          provider: @scheduled_job.provider,
          universe: nil,
          option_root: @scheduled_job.settings.fetch("option_root"),
          delete_sources: false,
          destination: nil,
          artifacts: [],
          retain_local: {}
        )
        bridge_job = Tickrake::ScheduledJobConfig.new(
          name: @scheduled_job.name,
          type: "maintenance",
          provider: @scheduled_job.provider,
          interval_seconds: @scheduled_job.interval_seconds,
          windows: @scheduled_job.windows,
          run_at: @scheduled_job.run_at,
          days: @scheduled_job.days,
          lookback_days: nil,
          dte_buckets: [],
          universe: [],
          tasks: [task],
          task: nil,
          settings: {},
          manual: @scheduled_job.manual
        )

        result = Tickrake::MaintenanceJob.new(
          @runtime,
          scheduled_job: bridge_job,
          start_date: @start_date,
          end_date: @end_date,
          progress_reporter: @progress_reporter
        ).run(now: now)

        Result.new(task: "compact_option_samples", processed_dates: result.processed_dates, artifacts_written: result.artifacts_written)
      ensure
        @progress_reporter&.finish
      end
    end
  end
end
