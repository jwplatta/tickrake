# frozen_string_literal: true

require "digest"
require "duckdb"
require "pathname"
require "time"

module Tickrake
  module Storage
    class HistoricalPublisher
      VALID_DATASETS = %w[options candles level_one order_book fundamentals economic_events].freeze

      def initialize(runtime:, datastore_name: nil)
        @runtime = runtime
        @config = runtime.config
        @datastore_name = datastore_name || @config.historical_datastore || "minio_historical"
        @datastore_config = @config.datastore(@datastore_name)
        @store = Storage::S3Archive.new(@config, datastore_config: @datastore_config)
        @manifest_writer = HistoricalManifestWriter.new(@store)
        @data_dir = Tickrake::PathSupport.expand_path(@config.data_dir)
      end

      def scan_and_publish(dataset_types: nil, batch_size: nil)
        target_datasets = normalize_datasets(dataset_types)
        @runtime.logger.info("historical_publisher: starting scan for datasets: #{target_datasets.join(', ')}")

        files_scanned = 0
        files_uploaded = 0
        manifest_entries_by_dataset = Hash.new { |h, k| h[k] = [] }

        target_datasets.each do |dataset_type|
          dataset_dir = File.join(@data_dir, dataset_type)
          next unless Dir.exist?(dataset_dir)

          pattern = File.join(dataset_dir, "**", "*.parquet")
          Dir.glob(pattern).each do |file_path|
            files_scanned += 1
            break if batch_size && files_uploaded >= batch_size

            result = process_file(dataset_type, file_path)
            if result
              files_uploaded += 1
              manifest_entries_by_dataset[dataset_type] << result
            end
          end
        end

        # Upsert changed entries into MinIO audit manifests
        manifest_entries_by_dataset.each do |dataset_type, entries|
          next if entries.empty?

          @manifest_writer.upsert_entries(dataset_type, entries)
          @runtime.logger.info("historical_publisher: updated manifest for #{dataset_type} with #{entries.size} file(s)")
        end

        summary = {
          files_scanned: files_scanned,
          files_uploaded: files_uploaded,
          datasets_updated: manifest_entries_by_dataset.keys
        }
        @runtime.logger.info("historical_publisher: finished scan. #{summary.inspect}")
        summary
      end

      def process_file(dataset_type, file_path)
        file_size = File.size(file_path)
        checksum = Digest::SHA256.file(file_path).hexdigest
        remote_key = relative_key_for(file_path)

        # Check ledger
        existing = Models::HistoricalFileLedger.find_by(remote_key: remote_key)
        if existing && existing.checksum == checksum && existing.file_size == file_size
          return nil # Up to date, skip upload
        end

        # Extract attributes from path
        meta = extract_metadata(dataset_type, file_path)
        row_count = count_rows(file_path)

        # Upload file to MinIO
        uploaded_obj = @store.upload_file(file_path, key: remote_key)
        remote_uri = "s3://#{uploaded_obj.bucket}/#{uploaded_obj.key}"

        now = Time.now.utc.iso8601
        Models::HistoricalFileLedger.record_publication!(
          dataset_type: dataset_type,
          provider: meta[:provider],
          symbol: meta[:symbol],
          frequency: meta[:frequency],
          date_or_year: meta[:date_or_year],
          local_path: file_path,
          remote_bucket: uploaded_obj.bucket,
          remote_key: uploaded_obj.key,
          remote_uri: remote_uri,
          file_size: file_size,
          row_count: row_count,
          checksum: checksum,
          published_at: now
        )

        @runtime.logger.info("historical_publisher: published #{file_path} -> #{remote_uri} (#{row_count} rows)")

        {
          "key" => remote_key,
          "uri" => remote_uri,
          "file_size" => file_size,
          "row_count" => row_count,
          "checksum" => checksum,
          "published_at" => now
        }
      end

      def extract_metadata(dataset_type, file_path)
        rel = relative_key_for(file_path)
        parts = rel.split("/") # e.g. ["options", "massive", "2025", "03", "03", "SPXW_samples_2025-03-03.parquet"]

        case dataset_type
        when "options"
          # options/<provider>/<YYYY>/<MM>/<DD>/<root>_samples_<date>.parquet
          provider = parts[1]
          date = parts.length >= 5 ? parts[2..4].join("-") : nil
          filename = parts.last
          root = filename.split("_").first
          { provider: provider || "unknown", symbol: root || "unknown", date_or_year: date, frequency: nil }
        when "candles"
          # candles/<provider>/<frequency>/<year>/<symbol>.parquet
          provider = parts[1]
          frequency = parts[2]
          year = parts[3]
          symbol = File.basename(parts.last, ".parquet")
          { provider: provider || "unknown", symbol: symbol, date_or_year: year, frequency: frequency }
        when "level_one", "order_book"
          # level_one/<provider>/<YYYY>/<MM>/<DD>/<symbol>_<timestamp>Z.parquet
          provider = parts[1]
          date = parts.length >= 5 ? parts[2..4].join("-") : nil
          filename = parts.last
          symbol = filename.split("_").first
          { provider: provider || "unknown", symbol: symbol || "unknown", date_or_year: date, frequency: nil }
        when "fundamentals"
          # fundamentals/<provider>/<YYYY>/<MM>/<DD>.parquet
          provider = parts[1]
          date = parts.length >= 5 ? [parts[2], parts[3], File.basename(parts[4], ".parquet")].join("-") : nil
          { provider: provider || "unknown", symbol: "ALL", date_or_year: date, frequency: nil }
        when "economic_events"
          # economic_events/<source>/<category>/<YYYY>/<MM>/<DD>.parquet
          source = parts[1]
          category = parts[2]
          date = parts.length >= 6 ? [parts[3], parts[4], File.basename(parts[5], ".parquet")].join("-") : nil
          { provider: source || "unknown", symbol: category || "all", date_or_year: date, frequency: nil }
        else
          { provider: "unknown", symbol: "unknown", date_or_year: nil, frequency: nil }
        end
      end

      def count_rows(file_path)
        escaped_path = file_path.to_s.gsub("'", "''")
        DuckDB::Database.open do |db|
          db.connect do |con|
            res = con.query("SELECT COUNT(*) FROM read_parquet('#{escaped_path}')")
            res.first&.first.to_i
          end
        end
      rescue StandardError => e
        @runtime.logger.warn("historical_publisher: DuckDB count failed for #{file_path}: #{e.message}, falling back to 0")
        0
      end

      def relative_key_for(file_path)
        Pathname.new(file_path).relative_path_from(Pathname.new(@data_dir)).to_s
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
