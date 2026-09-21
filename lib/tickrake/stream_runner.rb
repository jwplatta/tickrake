# frozen_string_literal: true

module Tickrake
  class StreamRunner
    POLL_INTERVAL_SECONDS = 10

    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
      @job = StreamJob.new(runtime, scheduled_job: scheduled_job)
      @stream_thread = nil
      @shutdown_requested = false
      @in_session = false
    end

    def run
      Tickrake::Lockfile.new("tickrake-#{@scheduled_job.name}").synchronize do
        install_signal_handlers
        @runtime.logger.info({ msg: "[stream:#{@scheduled_job.name}] Starting consolidated stream runner.", event: "runner_start", pid: Process.pid })
        @runtime.with_timezone do
          loop do
            now = Time.now

            if @shutdown_requested
              stop_session if @in_session
              break
            end

            any_in_win = any_subscription_in_window?(now)

            if any_in_win && !@in_session
              start_session(now)
            elsif !any_in_win && @in_session
              stop_session
            elsif @in_session
              # Dynamically add/remove symbols for subscriptions as their windows open/close
              @job.sync_subscriptions(now)
            end

            sleep(POLL_INTERVAL_SECONDS)
          end
        end
        @runtime.logger.info({ msg: "[stream:#{@scheduled_job.name}] Consolidated stream runner stopped.", event: "runner_stop", reason: @shutdown_reason || "clean_exit" })
      end
    ensure
      stop_session if @in_session
      @job.close
      Tickrake::JobRegistry.new.delete(@scheduled_job.name)
    end

    private

    def start_session(window_start)
      @in_session = true
      @runtime.logger.info({ msg: "[stream:#{@scheduled_job.name}] Window opened for at least one subscription, starting stream session.", event: "session_start" })
      @stream_thread = Thread.new do
        @job.run_session(window_start: window_start)
      rescue StandardError => e
        @runtime.logger.error({
          msg: "[stream:#{@scheduled_job.name}] Session error: #{e.class}: #{e.message}",
          event: "session_error",
          error_class: e.class.name,
          error_message: e.message,
          backtrace: Array(e.backtrace).first(5).join(" | ")
        })
      ensure
        @in_session = false
      end
    end

    def stop_session
      return unless @in_session

      @runtime.logger.info({ msg: "[stream:#{@scheduled_job.name}] All subscription windows closed or shutdown requested, stopping stream session.", event: "session_stop" })
      @job.stop
      @stream_thread&.join(60)
      @stream_thread = nil
      @in_session = false
    end

    def any_subscription_in_window?(time)
      stream_config = @scheduled_job.settings
      stream_config.subscriptions.any? { |sub| sub.in_window?(time) }
    end

    def install_signal_handlers
      %w[TERM INT].each do |signal|
        Signal.trap(signal) do
          @shutdown_requested = true
          @shutdown_reason = signal
        end
      end
    end
  end
end
