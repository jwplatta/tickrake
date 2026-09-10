# frozen_string_literal: true

module Tickrake
  class OrderBookRunner
    POLL_INTERVAL_SECONDS = 10

    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
      @job = OrderBookJob.new(runtime, scheduled_job: scheduled_job)
      @stream_thread = nil
      @shutdown_requested = false
      @in_session = false
    end

    def run
      Tickrake::Lockfile.new("tickrake-#{@scheduled_job.name}").synchronize do
        install_signal_handlers
        @runtime.logger.info("[order_book:#{@scheduled_job.name}] Starting order book runner.")
        @runtime.with_timezone do
          loop do
            now = Time.now

            if @shutdown_requested
              stop_session if @in_session
              break
            end

            if in_window?(now) && !@in_session
              start_session(now)
            elsif !in_window?(now) && @in_session
              stop_session
            end

            sleep(POLL_INTERVAL_SECONDS)
          end
        end
        @runtime.logger.info("[order_book:#{@scheduled_job.name}] Order book runner stopped.")
      end
    ensure
      stop_session if @in_session
      @job.close
      Tickrake::JobRegistry.new.delete(@scheduled_job.name)
    end

    private

    def start_session(window_start)
      @in_session = true
      @runtime.logger.info("[order_book:#{@scheduled_job.name}] Window opened, starting session.")
      @stream_thread = Thread.new do
        @job.run_session(window_start: window_start)
      rescue StandardError => e
        @runtime.logger.error("[order_book:#{@scheduled_job.name}] Session error: #{e.class}: #{e.message}")
        @runtime.logger.error(Array(e.backtrace).first(5).join("\n"))
      ensure
        @in_session = false
      end
    end

    def stop_session
      return unless @in_session

      @runtime.logger.info("[order_book:#{@scheduled_job.name}] Window closed or shutdown, stopping session.")
      @job.stop
      @stream_thread&.join(60)
      @stream_thread = nil
      @in_session = false
    end

    def in_window?(time)
      day = time.strftime("%a").downcase[0, 3]
      minutes = (time.hour * 60) + time.min

      @scheduled_job.windows.any? do |window|
        next false unless window.days.include?(day)

        start_minutes = (window.start_time[0] * 60) + window.start_time[1]
        end_minutes   = (window.end_time[0] * 60) + window.end_time[1]
        minutes >= start_minutes && minutes <= end_minutes
      end
    end

    def install_signal_handlers
      %w[TERM INT].each do |signal|
        Signal.trap(signal) do
          @shutdown_requested = true
        end
      end
    end
  end
end
