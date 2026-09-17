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

      # Combined sidecars (fetch_run + file_metadata in one file)
      sidecar_paths = Dir.glob(File.join(pending_dir, "*.sidecar.json")).first(batch_size)

      # Legacy metadata-only sidecars
      legacy_paths = Dir.glob(File.join(pending_dir, "*.meta.json")).first(batch_size)

      ingest_combined_sidecars(sidecar_paths) if sidecar_paths.any?
      ingest_legacy_metadata_sidecars(legacy_paths) if legacy_paths.any?

      @runtime.tracker.evict_stale_metadata(older_than_days: 10)
    end

    private

    def ingest_combined_sidecars(paths)
      sidecars = paths.map { |path| JSON.parse(File.read(path)) }

      fetch_runs = sidecars.map { |s| s.fetch("fetch_run") }
      metadata_list = sidecars.filter_map { |s| s["file_metadata"] }.map do |attrs|
        attrs = attrs.transform_keys(&:to_sym)
        attrs[:updated_at] = Time.iso8601(attrs[:updated_at]) if attrs[:updated_at].is_a?(String)
        attrs
      end

      @runtime.tracker.ingest_sidecars(fetch_runs: fetch_runs, metadata_list: metadata_list)
      paths.each { |path| File.delete(path) }

      @runtime.logger.info("metadata_sync: ingested #{paths.length} sidecar(s)")
    end

    def ingest_legacy_metadata_sidecars(paths)
      attrs_list = paths.map do |path|
        attrs = JSON.parse(File.read(path)).transform_keys(&:to_sym)
        attrs[:updated_at] = Time.iso8601(attrs[:updated_at]) if attrs[:updated_at].is_a?(String)
        attrs
      end

      @runtime.tracker.bulk_upsert_file_metadata(attrs_list)
      paths.each { |path| File.delete(path) }

      @runtime.logger.info("metadata_sync: ingested #{paths.length} legacy metadata sidecar(s)")
    end
  end
end
