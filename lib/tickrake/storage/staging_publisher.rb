# frozen_string_literal: true

require "pathname"
require "time"

module Tickrake
  module Storage
    class StagingPublisher
      VALID_DATASETS = %w[options candles level_one order_book fundamentals economic_events].freeze

      DATASET_PATTERNS = {
        "options"         => "**/*.csv",
        "candles"         => "**/*.csv",
        "level_one"       => "**/*.ndjson",
        "order_book"      => "**/*.ndjson",
        "fundamentals"    => "**/*.ndjson",
        "economic_events" => "**/*.ndjson"
      }.freeze

      def initialize(runtime:, datastore_name: nil)
        @runtime = runtime
        @config = runtime.config
        @datastore_name = datastore_name || @config.staging_datastore || "minio_staging"
        @datastore_config = @config.datastore(@datastore_name)
        @store = Storage::S3Archive.new(@config, datastore_config: @datastore_config)
        @data_dir = Tickrake::PathSupport.expand_path(@config.data_dir)
      end

      def scan_and_publish(dataset_types: nil, batch_size: nil, delete_after_upload: true)
        target_datasets = normalize_datasets(dataset_types)
        @runtime.logger.info("staging_publisher: starting scan for datasets: #{target_datasets.join(', ')} (delete_after_upload=#{delete_after_upload})")

        files_scanned = 0
        files_uploaded = 0
        files_deleted = 0

        target_datasets.each do |dataset_type|
          dataset_dir = File.join(@data_dir, dataset_type)
          next unless Dir.exist?(dataset_dir)

          pattern = File.join(dataset_dir, DATASET_PATTERNS.fetch(dataset_type, "**/*"))
          Dir.glob(pattern).each do |file_path|
            next if file_path.end_with?(".tmp")

            files_scanned += 1
            break if batch_size && files_uploaded >= batch_size

            uploaded, deleted = process_file(dataset_type, file_path, delete_after_upload: delete_after_upload)
            if uploaded
              files_uploaded += 1
              files_deleted += 1 if deleted
            end
          end
        end

        summary = {
          files_scanned: files_scanned,
          files_uploaded: files_uploaded,
          files_deleted: files_deleted
        }
        @runtime.logger.info("staging_publisher: finished scan. #{summary.inspect}")
        summary
      end

      def process_file(dataset_type, file_path, delete_after_upload: true)
        file_size = File.size(file_path)
        remote_key = relative_key_for(file_path)
        meta = extract_metadata(dataset_type, file_path)

        started_at = Time.now
        uploaded_obj = @store.upload_file(file_path, key: remote_key)
        duration_ms = ((Time.now - started_at) * 1000).round
        remote_uri = "s3://#{uploaded_obj.bucket}/#{uploaded_obj.key}"

        deleted = false
        if delete_after_upload
          begin
            File.unlink(file_path) if File.exist?(file_path)
            deleted = true
          rescue StandardError => e
            @runtime.logger.warn("staging_publisher: failed to delete local file #{file_path}: #{e.message}")
          end
        end

        @runtime.logger.info({
          msg: "staging_upload",
          event: "staging_upload",
          data_type: dataset_type,
          provider: meta[:provider],
          symbol: meta[:symbol],
          date: meta[:date],
          filename: File.basename(file_path),
          remote_uri: remote_uri,
          byte_count: file_size,
          duration_ms: duration_ms,
          purged_local_file: deleted
        })

        [true, deleted]
      rescue StandardError => e
        @runtime.logger.error("staging_publisher: failed to upload #{file_path}: #{e.message}")
        [false, false]
      end

      def relative_key_for(file_path)
        Pathname.new(file_path).relative_path_from(Pathname.new(@data_dir)).to_s
      end

      def extract_metadata(dataset_type, file_path)
        rel = relative_key_for(file_path)
        parts = rel.split("/")

        case dataset_type
        when "options"
          # options/<provider>/<YYYY>/<MM>/<DD>/<root>_exp<expiration>_<date>_<time>.csv
          provider = parts[1]
          date = parts.length >= 5 ? parts[2..4].join("-") : nil
          root = parts.last.to_s.split("_").first
          { provider: provider || "unknown", symbol: root || "unknown", date: date }
        when "candles"
          # candles/<provider>/<frequency>/<symbol>.csv
          provider = parts[1]
          symbol = File.basename(parts.last, ".csv")
          { provider: provider || "unknown", symbol: symbol, date: nil }
        when "level_one", "order_book"
          # level_one/<provider>/<YYYY>/<MM>/<DD>/<symbol>.ndjson
          provider = parts[1]
          date = parts.length >= 5 ? parts[2..4].join("-") : nil
          symbol = File.basename(parts.last, ".ndjson").split("_").first
          { provider: provider || "unknown", symbol: symbol || "unknown", date: date }
        when "fundamentals"
          # fundamentals/<provider>/<YYYY>/<MM>/<DD>.ndjson
          provider = parts[1]
          date = parts.length >= 5 ? [parts[2], parts[3], File.basename(parts[4], ".ndjson")].join("-") : nil
          { provider: provider || "unknown", symbol: "ALL", date: date }
        when "economic_events"
          # economic_events/<source>/<category>/<YYYY>/<MM>/<DD>.ndjson
          source = parts[1]
          category = parts[2]
          date = parts.length >= 6 ? [parts[3], parts[4], File.basename(parts[5], ".ndjson")].join("-") : nil
          { provider: source || "unknown", symbol: category || "all", date: date }
        else
          { provider: "unknown", symbol: "unknown", date: nil }
        end
      end

      private

      def normalize_datasets(datasets)
        return VALID_DATASETS if datasets.nil? || datasets.empty?

        list = Array(datasets).flatten.map(&:to_s)
        return VALID_DATASETS if list.include?("all")

        list & VALID_DATASETS
      end
    end
  end
end
