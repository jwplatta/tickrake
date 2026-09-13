# frozen_string_literal: true

module Tickrake
  class ReconcilerJob
    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
    end

    def run
      s3_archive = build_s3_archive
      unless s3_archive
        @runtime.logger.warn("reconciler: s3_archive is not configured; skipping run")
        return
      end

      providers = reconcile_providers
      if providers.empty?
        @runtime.logger.warn("reconciler: no providers configured; skipping run")
        return
      end

      providers.each do |provider|
        run_options_pass(provider, s3_archive)
        run_candles_pass(provider, s3_archive)
      end
    end

    private

    def build_s3_archive
      archive_config = @runtime.config.s3_archive
      return nil unless archive_config

      Tickrake::Storage::S3Archive.new(@runtime.config)
    end

    def reconcile_providers
      configured = @scheduled_job.settings.fetch("providers", nil)
      return Array(configured) if configured

      @runtime.config.providers.map(&:name)
    end

    # ---------------------------------------------------------------------------
    # Options pass
    # ---------------------------------------------------------------------------

    def run_options_pass(provider, s3_archive)
      prefix = "manifests/options/#{provider}/"
      keys = s3_archive.list_keys(prefix: prefix)

      if keys.empty?
        @runtime.logger.info("reconciler: no option manifests found under #{prefix}")
        return
      end

      manifests = keys.flat_map do |key|
        raw = s3_archive.download_content(key)
        JSON.parse(raw)
      rescue StandardError => e
        @runtime.logger.warn("reconciler: failed to parse manifest #{key}: #{e.message}")
        []
      end

      by_root = manifests.group_by { |m| m.fetch("root") }

      writer = Tickrake::Index::AtomicJsonWriter.new

      by_root.each do |root, root_manifests|
        payload = build_root_index_payload(provider, root, root_manifests)
        local_path = root_index_local_path(provider, root)
        writer.write(local_path, payload)
        s3_archive.upload(local_path)
        @runtime.logger.info("reconciler: wrote ROOT.json for provider=#{provider} root=#{root} entries=#{root_manifests.length}")
      end

      roots = by_root.keys.sort
      write_tickers_index(provider, roots, s3_archive)
      write_tickers_cache(provider, roots)
    end

    def build_root_index_payload(provider, root, manifests)
      historical = manifests.map do |m|
        files = m.fetch("artifacts", {}).map do |format, artifact|
          {
            "format"    => format,
            "uri"       => artifact.fetch("uri"),
            "row_count" => artifact.fetch("row_count", nil)
          }.compact
        end

        {
          "sample_date"  => m.fetch("sample_date"),
          "archived_at"  => m.fetch("archived_at"),
          "files"        => files
        }
      end.sort_by { |e| e.fetch("sample_date") }

      {
        "provider"       => provider,
        "root"           => root,
        "updated_at"     => Time.now.utc.iso8601,
        "historical"     => historical
      }
    end

    def root_index_local_path(provider, root)
      File.join(@runtime.config.options_dir, provider, "#{root}.json")
    end

    def write_tickers_index(provider, roots, s3_archive)
      payload = {
        "provider"       => provider,
        "updated_at"     => Time.now.utc.iso8601,
        "roots"          => roots
      }
      local_path = tickers_index_local_path(provider)
      Tickrake::Index::AtomicJsonWriter.new.write(local_path, payload)
      s3_archive.upload(local_path)
      @runtime.logger.info("reconciler: wrote tickers.json for provider=#{provider} roots=#{roots.length}")
    end

    def tickers_index_local_path(provider)
      File.join(@runtime.config.options_dir, provider, "tickers.json")
    end

    def write_tickers_cache(provider, roots)
      cache_dir = File.join(File.expand_path("~/.tickrake/data/index_cache"), provider)
      FileUtils.mkdir_p(cache_dir)
      cache_path = File.join(cache_dir, "tickers_cache.json")
      payload = {
        "generated_at" => Time.now.utc.iso8601,
        "roots"        => roots
      }
      Tickrake::Index::AtomicJsonWriter.new.write(cache_path, payload)
      @runtime.logger.info("reconciler: wrote tickers cache for provider=#{provider} path=#{cache_path}")
    end

    # ---------------------------------------------------------------------------
    # Candles pass
    # ---------------------------------------------------------------------------

    def run_candles_pass(provider, s3_archive)
      prefix = "manifests/candles/#{provider}/"
      keys = s3_archive.list_keys(prefix: prefix)

      if keys.empty?
        @runtime.logger.debug("reconciler: no candle manifests found under #{prefix}")
        return
      end

      manifests = keys.flat_map do |key|
        raw = s3_archive.download_content(key)
        JSON.parse(raw)
      rescue StandardError => e
        @runtime.logger.warn("reconciler: failed to parse candle manifest #{key}: #{e.message}")
        []
      end

      # Group by (ticker, frequency) and keep only the latest archived_at per group.
      by_ticker_freq = manifests.group_by { |m| [m.fetch("ticker", nil), m.fetch("frequency", nil)] }
      by_ticker_freq.each do |(ticker, frequency), group|
        latest = group.max_by { |m| m.fetch("archived_at", "") }
        # TODO: build and upload a candle index entry once candle data lands in S3.
        @runtime.logger.debug(
          "reconciler: candle manifest found provider=#{provider} ticker=#{ticker} " \
          "frequency=#{frequency} archived_at=#{latest&.fetch('archived_at', nil)}"
        )
      end
    end
  end
end
