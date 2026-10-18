# frozen_string_literal: true

module Tickrake
  module ScheduledRunnerSupport
    # Shared scheduler-only resilience flow for runner classes.
    #
    # Includers are expected to provide:
    # - @runtime
    # - @scheduled_job
    # - @sleeper
    # - @shutdown_requested
    # - due?(now)
    # - mark_iteration_success(now)
    # - log_iteration_failure(now, error)
    # - scheduler_log_prefix
    private

    def initialize_scheduled_runner_support
      @consecutive_failures = 0
    end

    def interruptible_sleep(seconds)
      remaining = seconds.to_f
      while remaining > 0 && !@shutdown_requested
        chunk = [remaining, 1].min
        @sleeper.sleep(chunk)
        remaining -= chunk
      end
    end

    def execute_iteration_with_resilience(now)
      return false unless due?(now)

      result = yield

      if iteration_successful?(result)
        mark_iteration_success(now)
        reset_consecutive_failures
      else
        log_iteration_result_failure(now, result)
        record_consecutive_failure(now, reason: "degraded run result")
        interruptible_sleep(@runtime.config.retry_delay_seconds)
      end
      true
    rescue Tickrake::SchedulerRestartRequired
      raise
    rescue StandardError => e
      log_iteration_failure(now, e)
      record_consecutive_failure(now, reason: "raised exception")
      interruptible_sleep(@runtime.config.retry_delay_seconds)
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

    def scheduled_provider_definitions
      @scheduled_provider_definitions ||= @runtime.config.provider_names_for_job(
        @scheduled_job,
        override_name: @runtime.provider_override_name
      ).map do |provider_name|
        @runtime.config.provider_definition(provider_name)
      end
    end

  end
end
