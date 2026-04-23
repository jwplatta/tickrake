# frozen_string_literal: true

module Tickrake
  class CandlesSchedulerRunner
    def initialize(runtime, scheduled_job:, sleeper: Kernel, from_config_start: false)
      @runtime = runtime
      @scheduled_job = scheduled_job
      @sleeper = sleeper
      @job = CandlesJob.new(runtime, from_config_start: from_config_start, scheduled_job: scheduled_job)
      @last_run_at = nil
      @last_run_on = nil
      @shutdown_requested = false
    end

    def run
      Tickrake::Lockfile.new("tickrake-#{@scheduled_job.name}").synchronize do
        install_signal_handlers
        @runtime.logger.info("Starting candle scheduler job #{@scheduled_job.name}.")
        @runtime.with_timezone do
          until @shutdown_requested
            now = Time.now
            run_iteration(now)
            break if @shutdown_requested

            @sleeper.sleep(sleep_seconds(now))
          end
        end
        @runtime.logger.info("Stopped candle scheduler job #{@scheduled_job.name}.")
      end
    ensure
      Tickrake::JobRegistry.new.delete(@scheduled_job.name)
    end

    def run_iteration(now)
      return false unless due?(now)

      begin
        @job.run(now: now)
        @last_run_at = now
        @last_run_on = now.to_date
      rescue StandardError => e
        log_iteration_failure(now, e)
        @sleeper.sleep(@runtime.config.retry_delay_seconds) unless @shutdown_requested
      end
      true
    end

    def due?(time)
      return due_for_interval_schedule?(time) if @scheduled_job.interval_schedule?

      due_for_daily_schedule?(time)
    end

    def sleep_seconds(now)
      return [@scheduled_job.interval_seconds / 2, 30].max if @scheduled_job.interval_schedule? && in_window?(now)

      30
    end

    private

    def due_for_interval_schedule?(time)
      return false unless in_window?(time)
      return true unless @last_run_at

      (time - @last_run_at) >= @scheduled_job.interval_seconds
    end

    def due_for_daily_schedule?(time)
      return false if @last_run_on == time.to_date
      return false unless @scheduled_job.days.include?(time.strftime("%a").downcase[0, 3])

      current_minutes = (time.hour * 60) + time.min
      target_minutes = (@scheduled_job.run_at[0] * 60) + @scheduled_job.run_at[1]
      current_minutes >= target_minutes
    end

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

    def install_signal_handlers
      %w[TERM INT].each do |signal|
        Signal.trap(signal) do
          @shutdown_requested = true
          @runtime.logger.info("Received #{signal}, stopping candle scheduler #{@scheduled_job.name} after current iteration.")
        end
      end
    end

    def log_iteration_failure(now, error)
      summary = Array(error.backtrace).first(3).join(" | ")
      @runtime.logger.error(
        "Candle scheduler #{@scheduled_job.name} iteration failed at #{now.utc.iso8601}: #{error.class}: #{error.message}"
      )
      @runtime.logger.error("Backtrace: #{summary}") unless summary.empty?
    end
  end
end
