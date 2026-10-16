# frozen_string_literal: true

module Tickrake
  class HistoricalPublisherSchedulerRunner
    include ScheduledRunnerSupport

    def initialize(runtime, scheduled_job:, sleeper: Kernel)
      @runtime = runtime
      @scheduled_job = scheduled_job
      @sleeper = sleeper
      @job = HistoricalPublisherJob.new(runtime, scheduled_job: scheduled_job)
      @last_run_at = nil
      @shutdown_requested = false
      initialize_scheduled_runner_support
    end

    def run
      Tickrake::Lockfile.new("tickrake-#{@scheduled_job.name}").synchronize do
        install_signal_handlers
        @runtime.logger.info({ msg: "Starting historical_publisher scheduler job #{@scheduled_job.name}.", event: "runner_start", pid: Process.pid })
        @runtime.with_timezone do
          until @shutdown_requested
            now = Time.now
            run_iteration(now)
            break if @shutdown_requested

            interruptible_sleep(sleep_seconds(now))
          end
        end
        @runtime.logger.info({ msg: "Stopped historical_publisher scheduler job #{@scheduled_job.name}.", event: "runner_stop", reason: @shutdown_reason || "clean_exit" })
      end
    ensure
      Tickrake::JobRegistry.new.delete(@scheduled_job.name)
    end

    def run_iteration(now)
      execute_iteration_with_resilience(now) do
        @job.run
      end
    end

    def due?(time)
      return false unless in_window?(time)
      return true unless @last_run_at

      (time - @last_run_at) >= @scheduled_job.interval_seconds
    end

    def sleep_seconds(now)
      return [@scheduled_job.interval_seconds / 2, 30].max if in_window?(now)

      30
    end

    private

    def scheduler_log_prefix
      "Historical publisher scheduler"
    end
  end
end
