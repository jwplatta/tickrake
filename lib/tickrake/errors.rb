# frozen_string_literal: true

module Tickrake
  class Error < StandardError; end
  class ConfigError < Error; end
  class LockError < Error; end
  class SchedulerRestartRequired < Error
    EXIT_STATUS = 75

    attr_reader :provider_name, :job_name, :failure_count, :threshold, :cooldown_seconds

    def initialize(provider_name:, job_name:, failure_count:, threshold:, cooldown_seconds:)
      @provider_name = provider_name
      @job_name = job_name
      @failure_count = failure_count
      @threshold = threshold
      @cooldown_seconds = cooldown_seconds
      super(
        "Scheduler #{job_name} reached #{failure_count} consecutive #{provider_name} failures " \
        "(threshold=#{threshold}); exiting for supervised restart."
      )
    end

    def exit_status
      EXIT_STATUS
    end
  end
end
