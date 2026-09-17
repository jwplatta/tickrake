# frozen_string_literal: true

module Tickrake
  class MetadataSyncJob
    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
    end

    def run
      pending_dir = @runtime.config.pending_metadata_dir
      return 0 unless Dir.exist?(pending_dir)

      batch_size = @scheduled_job.settings.fetch("batch_size", 500)
      sidecar_paths = Dir.glob(File.join(pending_dir, "*.meta.json")).first(batch_size)
      return 0 if sidecar_paths.empty?

      sidecars = sidecar_paths.map { |path| JSON.parse(File.read(path)) }

      fetch_runs = sidecars.filter_map { |s| s["fetch_run"] }
      metadata_list = sidecars.filter_map { |s| s["file_metadata"] || (s["path"] ? s : nil) }.map do |attrs|
        attrs = attrs.transform_keys(&:to_sym)
        attrs[:updated_at] = Time.iso8601(attrs[:updated_at]) if attrs[:updated_at].is_a?(String)
        attrs
      end

      @runtime.tracker.ingest_sidecars(fetch_runs: fetch_runs, metadata_list: metadata_list)
      sidecar_paths.each { |path| File.delete(path) }

      @runtime.logger.info("metadata_sync: ingested #{sidecar_paths.length} sidecar(s)")

      @runtime.tracker.evict_stale_metadata(older_than_days: 10)

      sidecar_paths.length
    end
  end
end
