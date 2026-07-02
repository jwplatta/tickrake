# frozen_string_literal: true

module Tickrake
  class MaintenanceJob
    StepExecution = Struct.new(
      :action,
      :provider_name,
      :option_root,
      :sample_date,
      :success,
      :artifacts_written,
      :errors,
      keyword_init: true
    )

    Result = Struct.new(:processed_dates, :step_results, :artifacts_written, keyword_init: true) do
      def successful?
        step_results.all?(&:success)
      end

      def failure_count
        step_results.count { |result| !result.success }
      end
    end

    def initialize(runtime, scheduled_job:, start_date: nil, end_date: nil, progress_reporter: nil)
      @runtime = runtime
      @scheduled_job = scheduled_job
      @start_date = start_date
      @end_date = end_date
      @progress_reporter = progress_reporter
    end

    def run(now: Time.now)
      @runtime.with_timezone do
        @runtime.logger.info("Starting maintenance job #{@scheduled_job.name} at #{now.utc.iso8601}")

        processed_dates = []
        step_results = []
        artifacts_written = []

        selected_dates(now).each do |sample_date|
          processed_dates << sample_date
          maintenance_targets.each do |target|
            provider_name = target.fetch(:provider_name)
            option_root = target.fetch(:option_root)

            Array(@scheduled_job.tasks).each do |step|
              next unless step_matches_target?(step, provider_name: provider_name, option_root: option_root)

              processor = Tickrake::Maintenance::OptionSamples::Processor.new(
                config: @runtime.config,
                tracker: @runtime.tracker,
                provider_name: provider_name,
                option_root: option_root,
                sample_date: sample_date,
                logger: @runtime.logger
              )

              result = run_step(processor: processor, step: step)
              artifacts_written.concat(step_artifacts(result))
              step_results << StepExecution.new(
                action: step.action,
                provider_name: provider_name,
                option_root: option_root,
                sample_date: sample_date,
                success: result.successful?,
                artifacts_written: step_artifacts(result),
                errors: step_errors(result)
              )
              break unless result.successful?
            end
          end
        end

        @runtime.logger.info(
          "Completed maintenance job #{@scheduled_job.name} at #{Time.now.utc.iso8601} " \
          "processed_dates=#{processed_dates.length} step_results=#{step_results.length} failures=#{step_results.count { |step| !step.success }}"
        )

        Result.new(
          processed_dates: processed_dates,
          step_results: step_results,
          artifacts_written: artifacts_written
        )
      end
    end

    private

    def run_step(processor:, step:)
      case step.action
      when "compact"
        processor.compact(delete_sources: step.delete_sources, progress_reporter: @progress_reporter)
      when "archive"
        processor.archive(
          destination_name: step.destination,
          artifacts: step.artifacts,
          retain_local: step.retain_local
        )
      else
        raise Tickrake::Error, "Unknown maintenance action `#{step.action}`."
      end
    end

    def step_artifacts(result)
      if result.respond_to?(:artifacts_written)
        result.artifacts_written
      elsif result.respond_to?(:archived_paths)
        result.archived_paths
      else
        []
      end
    end

    def step_errors(result)
      result.respond_to?(:errors) ? result.errors : []
    end

    def selected_dates(now)
      return [now.to_date] if @start_date.nil? && @end_date.nil?

      raise Tickrake::Error, "Maintenance runs require both --start-date and --end-date." if @start_date.nil? || @end_date.nil?
      raise Tickrake::Error, "--end-date must be on or after --start-date." if @end_date < @start_date

      (@start_date..@end_date).to_a
    end

    def option_roots_for(step)
      return [step.option_root] unless step.option_root.to_s.empty?

      @runtime.config.universe(step.universe).option_roots
    end

    def provider_name_for(step)
      @runtime.provider_override_name || step.provider || @scheduled_job.provider || @runtime.config.default_provider_name
    end

    def maintenance_targets
      @maintenance_targets ||= Array(@scheduled_job.tasks).flat_map do |step|
        option_roots_for(step).map do |option_root|
          {
            provider_name: provider_name_for(step),
            option_root: option_root
          }
        end
      end.uniq
    end

    def step_matches_target?(step, provider_name:, option_root:)
      provider_name_for(step) == provider_name && option_roots_for(step).include?(option_root)
    end
  end
end
