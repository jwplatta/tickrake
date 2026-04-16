# frozen_string_literal: true

module Tickrake
  class OptionsMonitorRunner
    def initialize(runtime, scheduled_job:, sleeper: Kernel)
      @runtime = runtime
      @scheduled_job = scheduled_job
      @sleeper = sleeper
      @job = OptionsJob.new(runtime, scheduled_job: scheduled_job)
      @last_run_at = nil
      @shutdown_requested = false
    end

    def run
      Tickrake::Lockfile.new("tickrake-#{@scheduled_job.name}").synchronize do
        install_signal_handlers
        @runtime.logger.info("Starting options scheduler job #{@scheduled_job.name}.")
        @runtime.with_timezone do
          until @shutdown_requested
            now = Time.now
            if due?(now)
              @job.run(now: now)
              @last_run_at = now
            end
            break if @shutdown_requested

            @sleeper.sleep(sleep_seconds(now))
          end
        end
        @runtime.logger.info("Stopped options scheduler job #{@scheduled_job.name}.")
      end
    ensure
      Tickrake::JobRegistry.new.delete(@scheduled_job.name)
    end

    def due?(time)
      return false unless in_window?(time)
      return true unless @last_run_at

      (time - @last_run_at) >= @scheduled_job.interval_seconds
    end

    def run_iteration(now)
      return false unless due?(now)

      @job.run(now: now)
      @last_run_at = now
      true
    end

    private

    def in_window?(time)
      day = time.strftime("%a").downcase[0, 3]
      minutes = (time.hour * 60) + time.min

      @scheduled_job.windows.any? do |window|
        next false unless window.days.include?(day)

        start_minutes = (window.start_time[0] * 60) + window.start_time[1]
        end_minutes = (window.end_time[0] * 60) + window.end_time[1]
        minutes >= start_minutes && minutes <= end_minutes
      end
    end

    def sleep_seconds(now)
      return 30 unless in_window?(now)

      [@scheduled_job.interval_seconds / 2, 30].max
    end

    def install_signal_handlers
      %w[TERM INT].each do |signal|
        Signal.trap(signal) do
          @shutdown_requested = true
          @runtime.logger.info("Received #{signal}, stopping options scheduler #{@scheduled_job.name} after current iteration.")
        end
      end
    end
  end
end
