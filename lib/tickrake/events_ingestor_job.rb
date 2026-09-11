# frozen_string_literal: true

module Tickrake
  class EventsIngestorJob
    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
      @settings = scheduled_job.settings
      @batch_size = @settings.fetch("batch_size", 10)
      @stale_age_seconds = @settings.fetch("stale_age_seconds", 1800)
      @datastore_name = @settings["datastore_name"]

      @pending_events_dir = runtime.config.pending_events_dir
      @storage_paths = Tickrake::Storage::Paths.new(runtime.config)
      @level_one_writer = Tickrake::Storage::LevelOneParquetWriter.new
      @order_book_writer = Tickrake::Storage::OrderBookParquetWriter.new
    end

    def run
      return unless Dir.exist?(@pending_events_dir)

      recover_stale_tmp_files

      files = Dir.glob(File.join(@pending_events_dir, "*.ndjson")).first(@batch_size)
      return if files.empty?

      total_events = 0
      files.each do |path|
        events = parse_ndjson(path)
        next if events.empty?

        ingest_file(events)
        total_events += events.size
        File.delete(path)
      rescue StandardError => e
        @runtime.logger.error("events_ingestor: failed to process #{File.basename(path)}: #{e.class}: #{e.message}")
      end

      @runtime.logger.info("events_ingestor: processed #{files.size} file(s), #{total_events} event(s)") if total_events > 0
    end

    private

    def recover_stale_tmp_files
      Dir.glob(File.join(@pending_events_dir, "*.ndjson.tmp")).each do |path|
        age = Time.now - File.mtime(path)
        next unless age >= @stale_age_seconds

        final_path = path.sub(/\.tmp$/, "")
        File.rename(path, final_path)
        @runtime.logger.warn("events_ingestor: recovered stale tmp file #{File.basename(final_path)}")
      end
    end

    def parse_ndjson(path)
      lines = File.readlines(path, chomp: true)
      lines.filter_map do |line|
        next if line.strip.empty?

        JSON.parse(line)
      rescue JSON::ParserError
        nil
      end
    end

    def ingest_file(events)
      job_type = events.first&.fetch("job_type", nil)
      return unless job_type

      by_provider_symbol = events.group_by { |e| [e["provider"], e["symbol"]] }

      by_provider_symbol.each do |(provider, symbol), group|
        flush_start = Time.at(group.map { |e| e["received_at"].to_i }.min / 1000.0)
        provider_definition = provider ? @runtime.config.provider_definition(provider) : nil

        case job_type
        when "level_one"
          path = @storage_paths.level_one_path(
            provider: provider, symbol: symbol,
            flush_start: flush_start, provider_definition: provider_definition
          )
          @level_one_writer.write(path, rows: group)
          @runtime.logger.info("events_ingestor: wrote #{path} (#{group.size} events)")
        when "order_book"
          path = @storage_paths.order_book_path(
            provider: provider, symbol: symbol,
            flush_start: flush_start, provider_definition: provider_definition
          )
          @order_book_writer.write(path, rows: group)
          @runtime.logger.info("events_ingestor: wrote #{path} (#{group.size} events)")
        else
          @runtime.logger.warn("events_ingestor: unknown job_type `#{job_type}` — skipping group")
          next
        end

        upload_to_s3(path) if @datastore_name
      rescue StandardError => e
        @runtime.logger.error("events_ingestor: failed to write #{provider}/#{symbol}: #{e.class}: #{e.message}")
      end
    end

    def upload_to_s3(local_path)
      datastore = @runtime.config.datastore(@datastore_name)
      Tickrake::Storage::S3Archive.new(@runtime.config, archive_config: datastore).upload(local_path)
    rescue StandardError => e
      @runtime.logger.error("events_ingestor: S3 upload failed for #{local_path}: #{e.message}")
    end
  end
end
