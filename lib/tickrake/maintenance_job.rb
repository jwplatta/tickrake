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

        option_steps = Array(@scheduled_job.tasks).select { |s| s.subject != "candles" }
        candle_steps = Array(@scheduled_job.tasks).select { |s| s.subject == "candles" }

        unless option_steps.empty?
          run_option_steps(now, option_steps, processed_dates: processed_dates,
                           step_results: step_results, artifacts_written: artifacts_written)
        end

        unless candle_steps.empty?
          run_candle_steps(candle_steps, step_results: step_results, artifacts_written: artifacts_written)
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

    def run_option_steps(now, steps, processed_dates:, step_results:, artifacts_written:)
      selected_dates(now).each do |sample_date|
        processed_dates << sample_date
        maintenance_targets(steps).each do |target|
          provider_name = target.fetch(:provider_name)
          option_root = target.fetch(:option_root)

          last_compact_result = nil
          steps.each do |step|
            next unless step_matches_target?(step, provider_name: provider_name, option_root: option_root)

            context = Tickrake::Maintenance::OptionSamples::Context.new(
              config: @runtime.config,
              tracker: @runtime.tracker,
              provider_name: provider_name,
              option_root: option_root,
              sample_date: sample_date,
              logger: @runtime.logger
            )

            result = run_step(context: context, step: step, last_compact_result: last_compact_result)
            last_compact_result = result if step.action == "compact" && result.successful?
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
            break if result.respond_to?(:skipped?) && result.skipped?
            break unless result.successful?
          end
        end
      end
    end

    def run_candle_steps(steps, step_results:, artifacts_written:)
      candle_targets.each do |target|
        context = Tickrake::Maintenance::Candles::Context.new(
          config: @runtime.config,
          provider_name: target[:provider_name],
          frequency: target[:frequency],
          symbol: target[:symbol],
          logger: @runtime.logger
        )

        last_compact_result = nil
        steps.each do |step|
          result = run_candle_step(context: context, step: step, last_compact_result: last_compact_result)
          last_compact_result = result if step.action == "compact" && result.successful?
          artifacts_written.concat(step_artifacts(result))
          step_results << StepExecution.new(
            action: step.action,
            provider_name: target[:provider_name],
            option_root: target[:symbol],
            sample_date: nil,
            success: result.successful?,
            artifacts_written: step_artifacts(result),
            errors: step_errors(result)
          )
          break if result.respond_to?(:skipped?) && result.skipped?
          break unless result.successful?
        end
      end
    end

    def run_candle_step(context:, step:, last_compact_result: nil)
      case step.action
      when "compact"
        Tickrake::Maintenance::Candles::Compactor.new(context: context).run
      when "archive"
        years = last_compact_result&.years_written || []
        return Tickrake::Maintenance::Candles::CompactResult.new(
          success: true, provider_name: context.provider_name,
          symbol: context.symbol, frequency: context.frequency,
          row_count: 0, years_written: [], errors: []
        ) if years.empty?

        s3_archive = archive_s3_archive_for_destination(step.destination)
        return Tickrake::Maintenance::Candles::ArchiveResult.new(
          success: false, provider_name: context.provider_name,
          symbol: context.symbol, frequency: context.frequency,
          artifact_results: [], errors: ["No S3 archive configured for #{step.destination}"]
        ) unless s3_archive

        Tickrake::Maintenance::Candles::Archiver.new(
          context: context, s3_archive: s3_archive
        ).upload(years: years)
      else
        raise Tickrake::Error, "Unknown maintenance action `#{step.action}`."
      end
    end

    def run_step(context:, step:, last_compact_result: nil)
      case step.action
      when "compact"
        run_compact_step(context: context, delete_sources: step.delete_sources)
      when "archive"
        run_archive_step(
          context: context,
          destination_name: step.destination,
          artifacts: step.artifacts,
          retain_local: step.retain_local,
          row_count: last_compact_result&.row_count
        )
      else
        raise Tickrake::Error, "Unknown maintenance action `#{step.action}`."
      end
    end

    def run_compact_step(context:, delete_sources:)
      compact = Tickrake::Maintenance::OptionSamples::Compactor.new(context: context).run(progress_reporter: @progress_reporter)
      return compact unless compact.successful?
      return compact if compact.artifacts_written.empty?

      validation = Tickrake::Maintenance::OptionSamples::Validator.new(context: context).run
      unless validation.safe_to_delete
        return Tickrake::Maintenance::OptionSamples::CompactResult.new(
          success: false,
          provider_name: context.provider_name,
          option_root: context.option_root,
          sample_date: context.sample_date,
          artifacts_written: compact.artifacts_written,
          row_count: compact.row_count,
          source_file_count: compact.source_file_count,
          errors: validation.errors
        )
      end

      if delete_sources
        cleanup = Tickrake::Maintenance::OptionSamples::SourceSampleCleaner.new(context: context).run(source_paths: validation.source_paths)
        return cleanup if !cleanup.successful?
      end

      compact
    end

    def run_archive_step(context:, destination_name:, artifacts:, retain_local:, row_count: nil)
      s3_archive = archive_s3_archive(context, destination_name)
      manifest_writer = s3_archive ? Tickrake::Maintenance::OptionSamples::ManifestWriter.new(s3_archive: s3_archive) : nil

      archive = Tickrake::Maintenance::OptionSamples::ArtifactArchiver.new(
        context: context,
        manifest_writer: manifest_writer
      ).upload(
        destination_name: destination_name,
        artifacts: artifacts,
        row_count: row_count
      )
      return archive unless archive.successful?

      retention = Tickrake::Maintenance::OptionSamples::LocalArtifactManager.new(context: context).apply(
        remote_uris: archive.remote_uris,
        retain_local: step_retain_local(retain_local),
        artifacts: artifacts
      )
      return retention if !retention.successful?

      archive
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

    def step_retain_local(retain_local)
      retain_local || {}
    end

    def selected_dates(now)
      return [now.to_date] if @start_date.nil? && @end_date.nil?

      raise Tickrake::Error, "Maintenance runs require both --start-date and --end-date." if @start_date.nil? || @end_date.nil?
      raise Tickrake::Error, "--end-date must be on or after --start-date." if @end_date < @start_date

      (@start_date..@end_date).to_a
    end

    def option_roots_for(step)
      return [step.option_root] unless step.option_root.to_s.empty?
      roots = []
      roots.concat(@runtime.config.universe(step.universe).option_roots) unless step.universe.to_s.empty?
      Array(step.universes).each do |name|
        roots.concat(@runtime.config.universe(name).option_roots)
      end
      Array(step.tickers).each do |row|
        entry = row.is_a?(String) ? Tickrake::UniverseEntry.new(symbol: row) : Tickrake::UniverseEntry.new(
          symbol: row.fetch("symbol"),
          option_root: row["option_root"],
          option_roots: Array(row["option_roots"]),
          start_date: nil,
          need_extended_hours_data: false,
          need_previous_close: false
        )
        roots.concat(
          if Array(entry.option_roots).any?
            entry.option_roots
          elsif !entry.option_root.to_s.empty?
            [entry.option_root]
          else
            [entry.symbol]
          end
        )
      end
      roots.map { |root| root.to_s.strip.upcase }.reject(&:empty?).uniq
    end

    def provider_name_for(step)
      @runtime.provider_override_name || step.provider || @scheduled_job.provider || @runtime.config.default_provider_name
    end

    def candle_targets
      candles_dir = @runtime.config.candles_dir
      return [] unless candles_dir && Dir.exist?(candles_dir)

      provider_name = @runtime.provider_override_name || @scheduled_job.provider || @runtime.config.default_provider_name
      provider_dir = File.join(candles_dir, provider_name.to_s)
      return [] unless Dir.exist?(provider_dir)

      Dir.glob(File.join(provider_dir, "**", "*.csv")).filter_map do |csv_path|
        relative = csv_path.delete_prefix("#{provider_dir}/")
        parts = relative.split("/")
        next unless parts.size == 2

        frequency = parts[0]
        symbol = File.basename(parts[1], ".csv")
        { provider_name: provider_name, frequency: frequency, symbol: symbol }
      end
    end

    def archive_s3_archive_for_destination(destination_name)
      archive_config = @runtime.config.archives[destination_name]
      return nil unless archive_config

      Tickrake::Storage::S3Archive.new(@runtime.config, archive_config: archive_config)
    end

    def maintenance_targets(steps = nil)
      steps ||= Array(@scheduled_job.tasks)
      @maintenance_targets ||= steps.flat_map do |step|
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

    def archive_s3_archive(context, destination_name)
      archive_config = context.config.archives[destination_name]
      return nil unless archive_config

      Tickrake::Storage::S3Archive.new(context.config, archive_config: archive_config)
    end
  end
end
