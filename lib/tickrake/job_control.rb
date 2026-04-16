# frozen_string_literal: true

module Tickrake
  class JobControl
    def initialize(registry: Tickrake::JobRegistry.new, starter: nil, stdout: $stdout)
      @registry = registry
      @starter = starter || Tickrake::BackgroundProcess.new(registry: registry, stdout: stdout)
      @stdout = stdout
    end

    def start(target:, config_path:, provider_name: nil, from_config_start: false)
      resolve_job_targets(target, config_path: config_path).each do |job_name|
        @starter.start(
          job_name: job_name,
          config_path: config_path,
          provider_name: provider_name,
          from_config_start: from_config_start
        )
      end
    end

    def stop(target:, config_path:, timeout_seconds: 5)
      resolve_job_targets(target, config_path: config_path).each do |job_name|
        stop_one(job_name, timeout_seconds: timeout_seconds)
      end
    end

    def restart(target:, config_path: Tickrake::PathSupport.config_path, provider_name: nil, from_config_start: nil)
      resolve_job_targets(target, config_path: config_path).each do |job_name|
        metadata = @registry.read(job_name) || {}
        stop_one(job_name, timeout_seconds: nil, waiting_message: restart_waiting_message(job_name))
        @starter.start(
          job_name: job_name,
          config_path: restart_config_path(config_path, metadata),
          provider_name: restart_provider_name(provider_name, metadata),
          from_config_start: restart_from_config_start(from_config_start, metadata)
        )
      end
    end

    private

    def resolve_job_targets(target, config_path:)
      return Tickrake::ConfigLoader.load(config_path).jobs.map(&:name) if target.to_s == "all"

      [Tickrake::ConfigLoader.load(config_path).job(target).name]
    end

    def stop_one(name, timeout_seconds:, waiting_message: nil)
      job = @registry.status(name)
      case job[:state]
      when "running"
        Process.kill("TERM", Integer(job[:pid]))
        wait_for_stop(name, Integer(job[:pid]), timeout_seconds: timeout_seconds, waiting_message: waiting_message)
      when "stale"
        @registry.delete(name)
        @stdout.puts("Removed stale #{name} job metadata for pid #{job[:pid]}.")
      else
        @stdout.puts("#{name} job is not running.")
      end
    end

    def wait_for_stop(name, pid, timeout_seconds:, waiting_message:)
      deadline = timeout_seconds && (Time.now + timeout_seconds)
      @stdout.puts(waiting_message) if waiting_message

      loop do
        unless @registry.pid_alive?(pid)
          @registry.delete(name)
          @stdout.puts("Stopped #{name} job (pid #{pid}).")
          return
        end

        break if deadline && Time.now >= deadline

        sleep 0.2
      end

      @stdout.puts("Sent TERM to #{name} job (pid #{pid}); waiting for shutdown.")
    end

    def restart_waiting_message(name)
      "Waiting for #{name} job to finish its current work before restarting. This can take a bit."
    end

    def restart_config_path(config_path, metadata)
      default = Tickrake::PathSupport.config_path
      return config_path if config_path != default

      metadata[:config_path] || config_path
    end

    def restart_provider_name(provider_name, metadata)
      provider_name || metadata[:provider_name]
    end

    def restart_from_config_start(from_config_start, metadata)
      return from_config_start unless from_config_start.nil?

      metadata[:from_config_start] == true
    end
  end
end
