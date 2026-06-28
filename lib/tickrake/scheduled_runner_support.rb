# frozen_string_literal: true

module Tickrake
  module ScheduledRunnerSupport
    private

    def initialize_scheduled_runner_support
      @consecutive_failures = 0
    end

    def execute_iteration_with_resilience(now)
      return false unless due?(now)

      result = with_provider_iteration_locks(now) do
        yield
      end
      return true if result == :deferred

      if iteration_successful?(result)
        mark_iteration_success(now)
        reset_consecutive_failures
      else
        log_iteration_result_failure(now, result)
        record_consecutive_failure(now, reason: "degraded run result")
        @sleeper.sleep(@runtime.config.retry_delay_seconds) unless @shutdown_requested
      end
      true
    rescue Tickrake::SchedulerRestartRequired
      raise
    rescue StandardError => e
      log_iteration_failure(now, e)
      record_consecutive_failure(now, reason: "raised exception")
      @sleeper.sleep(@runtime.config.retry_delay_seconds) unless @shutdown_requested
      true
    end

    def iteration_successful?(result)
      return true unless result.respond_to?(:successful?)

      result.successful?
    end

    def log_iteration_result_failure(now, result)
      @runtime.logger.error(
        "#{scheduler_log_prefix} #{@scheduled_job.name} iteration failed at #{now.utc.iso8601}: " \
        "degraded run result success_count=#{result.success_count} failure_count=#{result.failure_count}"
      )
    end

    def record_consecutive_failure(now, reason:)
      provider = restart_resilience_provider
      return unless provider

      @consecutive_failures += 1
      threshold = provider.restart_after_consecutive_failures
      cooldown = provider.restart_cooldown_seconds
      @runtime.logger.warn(
        "#{scheduler_log_prefix} #{@scheduled_job.name} counted a #{provider.name} failure at #{now.utc.iso8601} " \
        "due to #{reason}; consecutive_failures=#{@consecutive_failures}/#{threshold}"
      )
      return unless @consecutive_failures >= threshold

      @runtime.logger.error(
        "#{scheduler_log_prefix} #{@scheduled_job.name} reached #{provider.name} failure threshold " \
        "#{@consecutive_failures}/#{threshold}; exiting for restart with cooldown=#{cooldown}s."
      )
      raise Tickrake::SchedulerRestartRequired.new(
        provider_name: provider.name,
        job_name: @scheduled_job.name,
        failure_count: @consecutive_failures,
        threshold: threshold,
        cooldown_seconds: cooldown
      )
    end

    def reset_consecutive_failures
      return if @consecutive_failures.to_i.zero?

      @runtime.logger.info(
        "#{scheduler_log_prefix} #{@scheduled_job.name} reset consecutive failure count after a successful iteration."
      )
      @consecutive_failures = 0
    end

    def restart_resilience_provider
      scheduled_provider_definitions.find { |provider| !provider.restart_after_consecutive_failures.nil? }
    end

    def serialized_provider_names
      scheduled_provider_definitions.select(&:serialize_scheduled_jobs?).map(&:name).sort
    end

    def scheduled_provider_definitions
      @scheduled_provider_definitions ||= @runtime.config.provider_names_for_job(
        @scheduled_job,
        override_name: @runtime.provider_override_name
      ).map do |provider_name|
        @runtime.config.provider_definition(provider_name)
      end
    end

    def with_provider_iteration_locks(now, provider_names = serialized_provider_names, index = 0, &block)
      return yield if provider_names.empty?
      return yield if index >= provider_names.length

      provider_name = provider_names[index]
      Tickrake::Lockfile.new("tickrake-provider-#{provider_name}-scheduled").try_synchronize do
        return with_provider_iteration_locks(now, provider_names, index + 1, &block)
      end

      @runtime.logger.info(
        "#{scheduler_log_prefix} #{@scheduled_job.name} waiting for provider #{provider_name} scheduled iteration lock at #{now.utc.iso8601}."
      )
      :deferred
    end
  end
end
