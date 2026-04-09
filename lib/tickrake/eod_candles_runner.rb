# frozen_string_literal: true

module Tickrake
  class EodCandlesRunner
    def initialize(runtime, sleeper: Kernel, from_config_start: false)
      @runtime = runtime
      @sleeper = sleeper
      @job = CandlesJob.new(runtime, from_config_start: from_config_start)
      @last_run_on = nil
    end

    def run
      Tickrake::Lockfile.new("tickrake-eod-candles").synchronize do
        @runtime.logger.info("Starting candle scheduler job.")
        @runtime.with_timezone do
          loop do
            now = Time.now
            run_iteration(now)
            @sleeper.sleep(60)
          end
        end
      end
    end

    def run_iteration(now)
      return false unless due?(now)

      @job.run(now: now)
      @last_run_on = now.to_date
      true
    end

    def due?(time)
      return false if @last_run_on == time.to_date
      return false unless @runtime.config.eod_days.include?(time.strftime("%a").downcase[0, 3])

      current_minutes = (time.hour * 60) + time.min
      target_minutes = (@runtime.config.eod_run_at[0] * 60) + @runtime.config.eod_run_at[1]
      current_minutes >= target_minutes
    end
  end
end
