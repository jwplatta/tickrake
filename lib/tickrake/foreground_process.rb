# frozen_string_literal: true

module Tickrake
  class ForegroundProcess
    def initialize(stdout: $stdout)
      @stdout = stdout
    end

    def start(job_name:, config_path:, from_config_start: false, provider_name: nil, restart: false)
      config = Tickrake::ConfigLoader.load(config_path)
      job = config.job(job_name)
      runtime = Tickrake::Runtime.new(
        config: config,
        provider_name: provider_name,
        config_path: config_path
      )
      @stdout.puts("Starting #{job_name} in foreground.")
      Tickrake::JobRunner.run(runtime, job, from_config_start: from_config_start, restart: restart)
    end
  end
end
