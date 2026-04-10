# frozen_string_literal: true

module Tickrake
  class JobRegistry
    JOB_NAMES = %w[options candles].freeze

    def initialize
      FileUtils.mkdir_p(Tickrake::PathSupport.jobs_dir)
    end

    def write(name, attributes)
      File.write(state_path(name), JSON.pretty_generate(attributes))
    end

    def read(name)
      path = state_path(name)
      return unless File.exist?(path)

      JSON.parse(File.read(path), symbolize_names: true)
    end

    def delete(name)
      path = state_path(name)
      File.delete(path) if File.exist?(path)
    end

    def status(name)
      metadata = read(name)
      return { name: name, state: "stopped" } unless metadata

      pid = metadata[:pid]
      alive = pid_alive?(pid)
      metadata.merge(name: name, state: alive ? "running" : "stale")
    end

    def statuses
      JOB_NAMES.map { |name| status(name) }
    end

    def pid_alive?(pid)
      return false if pid.nil?

      Process.kill(0, Integer(pid))
      true
    rescue Errno::ESRCH, Errno::EPERM, ArgumentError, TypeError
      false
    end

    private

    def state_path(name)
      Tickrake::PathSupport.job_state_path(name)
    end
  end
end
