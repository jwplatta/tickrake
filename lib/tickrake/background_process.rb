# frozen_string_literal: true

require "rbconfig"

module Tickrake
  class BackgroundProcess
    def initialize(registry: Tickrake::JobRegistry.new, stdout: $stdout)
      @registry = registry
      @stdout = stdout
    end

    def start(job_name:, config_path:, from_config_start: false, provider_name: nil, restart: false)
      current = @registry.status(job_name)
      if current[:state] == "running"
        raise Tickrake::Error, "#{job_name} job is already running with pid #{current[:pid]}."
      end

      log_path = Tickrake::PathSupport.named_log_path(job_name)
      FileUtils.mkdir_p(File.dirname(log_path))
      log_device = File.open(log_path, "a")

      args = [
        RbConfig.ruby,
        executable_path,
        "run",
        "--job",
        job_name,
        restart ? "--supervisor" : "--scheduler",
        "--config",
        Tickrake::PathSupport.expand_path(config_path)
      ]
      args += ["--provider", provider_name] if provider_name
      args << "--from-config-start" if from_config_start

      pid = Process.spawn(*args, out: log_device, err: log_device, pgroup: true)
      Process.detach(pid)
      log_device.close

      @registry.write(
        job_name,
        pid: pid,
        command: args.join(" "),
        started_at: Time.now.utc.iso8601,
        config_path: Tickrake::PathSupport.expand_path(config_path),
        provider_name: provider_name,
        from_config_start: from_config_start,
        restart: restart,
        log_path: log_path
      )

      @stdout.puts("Started #{job_name} job with pid #{pid}.")
      pid
    end

    private

    def executable_path
      File.expand_path("../../exe/tickrake", __dir__)
    end
  end
end
