# frozen_string_literal: true

require "rbconfig"

module Tickrake
  class SchedulerSupervisor
    RESTART_DELAY_SECONDS = 5

    def initialize(runtime, scheduled_job:, from_config_start: false, sleeper: Kernel)
      @runtime = runtime
      @scheduled_job = scheduled_job
      @from_config_start = from_config_start
      @sleeper = sleeper
      @shutdown_requested = false
      @child_pid = nil
    end

    def run
      install_signal_handlers
      @runtime.logger.info("Starting scheduler supervisor for #{@scheduled_job.name}.")

      until @shutdown_requested
        exit_status = spawn_scheduler
        break if @shutdown_requested

        if exit_status.success?
          @runtime.logger.info("Scheduler #{@scheduled_job.name} exited cleanly; supervisor stopping.")
          break
        end

        @runtime.logger.error(
          "Scheduler #{@scheduled_job.name} exited unexpectedly with status=#{exit_status.exitstatus}; restarting in #{RESTART_DELAY_SECONDS}s."
        )
        @sleeper.sleep(RESTART_DELAY_SECONDS)
      end
    ensure
      @runtime.logger.info("Stopped scheduler supervisor for #{@scheduled_job.name}.")
    end

    private

    def spawn_scheduler
      @child_pid = Process.spawn(*scheduler_args)
      _, status = Process.wait2(@child_pid)
      status
    ensure
      @child_pid = nil
    end

    def scheduler_args
      args = [
        RbConfig.ruby,
        executable_path,
        "run",
        "--job",
        @scheduled_job.name,
        "--scheduler",
        "--config",
        @runtime.config_path
      ]
      args += ["--provider", @runtime.provider_override_name] if @runtime.provider_override_name
      args << "--from-config-start" if @from_config_start
      args
    end

    def executable_path
      File.expand_path("../../exe/tickrake", __dir__)
    end

    def install_signal_handlers
      %w[TERM INT].each do |signal|
        Signal.trap(signal) do
          @shutdown_requested = true
          if @child_pid
            Process.kill(signal, @child_pid)
            @runtime.logger.info("Received #{signal}, forwarding to scheduler #{@scheduled_job.name} (pid #{@child_pid}).")
          else
            @runtime.logger.info("Received #{signal}, stopping supervisor for #{@scheduled_job.name}.")
          end
        rescue Errno::ESRCH
          nil
        end
      end
    end
  end
end
