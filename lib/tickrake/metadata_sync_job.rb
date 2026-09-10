# frozen_string_literal: true

module Tickrake
  class MetadataSyncJob
    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
    end

    def run
      pending_dir = @runtime.config.pending_metadata_dir
      return unless Dir.exist?(pending_dir)

      batch_size = @scheduled_job.settings.fetch("batch_size", 500)
      sidecar_paths = Dir.glob(File.join(pending_dir, "*.meta.json")).first(batch_size)
      return if sidecar_paths.empty?

      attrs_list = sidecar_paths.map do |path|
        JSON.parse(File.read(path)).transform_keys(&:to_sym)
      end

      @runtime.tracker.bulk_upsert_file_metadata(attrs_list)
      sidecar_paths.each { |path| File.delete(path) }

      @runtime.logger.info("metadata_sync: ingested #{sidecar_paths.length} sidecar(s)")
    end
  end
end
