# frozen_string_literal: true

require "date"

module Tickrake
  module Storage
    class StagingPurger
      VALID_DATASETS = StagingPublisher::VALID_DATASETS

      attr_reader :store, :datastore_name

      def initialize(runtime:, datastore_name: nil, datastore_config: nil)
        @runtime = runtime
        @config = runtime.config
        @datastore_name = datastore_name || @config.staging_datastore || "minio_staging"
        @datastore_config = datastore_config || @config.datastore(@datastore_name)
        raise Tickrake::Error, "Datastore '#{@datastore_name}' not configured." unless @datastore_config

        @store = Storage::S3Archive.new(@config, datastore_config: @datastore_config)
        @data_dir = Tickrake::PathSupport.expand_path(@config.data_dir)
      end

      # Purges data matching dataset, date, and optional provider/symbol filters.
      # Targets can include remote staging bucket (default if neither specified or both), and/or local data dir.
      def purge(dataset_types: nil, date: nil, provider: nil, dry_run: false, purge_remote: true, purge_local: false)
        raise ArgumentError, "date (YYYY-MM-DD) is required for purge" unless date

        target_date = date.is_a?(Date) ? date : Date.iso8601(date.to_s)
        target_datasets = normalize_datasets(dataset_types)

        summary = {
          remote_keys_scanned: 0,
          remote_keys_deleted: 0,
          local_files_scanned: 0,
          local_files_deleted: 0,
          dry_run: dry_run
        }

        target_datasets.each do |dataset_type|
          if purge_remote
            purge_remote_dataset(dataset_type, target_date, provider: provider, dry_run: dry_run, summary: summary)
          end

          if purge_local
            purge_local_dataset(dataset_type, target_date, provider: provider, dry_run: dry_run, summary: summary)
          end
        end

        summary
      end

      def build_prefix(dataset_type, target_date, provider: nil)
        year = target_date.strftime("%Y")
        month = target_date.strftime("%m")
        day = target_date.strftime("%d")

        case dataset_type
        when "options", "candles", "level_one", "order_book"
          if provider && !provider.empty?
            "#{dataset_type}/#{provider}/#{year}/#{month}/#{day}/"
          else
            "#{dataset_type}/"
          end
        when "fundamentals"
          if provider && !provider.empty?
            "fundamentals/#{provider}/#{year}/#{month}/#{day}.ndjson"
          else
            "fundamentals/"
          end
        when "economic_events"
          if provider && !provider.empty?
            "economic_events/#{provider}/"
          else
            "economic_events/"
          end
        else
          "#{dataset_type}/"
        end
      end

      def matches_criteria?(key, dataset_type, target_date, provider: nil)
        parts = key.split("/")
        return false if parts.first != dataset_type

        case dataset_type
        when "options", "level_one", "order_book"
          # <dataset>/<provider>/<YYYY>/<MM>/<DD>/...
          return false if parts.length < 5
          return false if provider && !provider.empty? && parts[1] != provider

          file_date_str = parts[2..4].join("-")
          file_date_str == target_date.strftime("%Y-%m-%d")
        when "candles"
          # candles/<provider>/<YYYY>/<MM>/<DD>/<frequency>/<symbol>.csv
          return false if parts.length < 6
          return false if provider && !provider.empty? && parts[1] != provider

          file_date_str = parts[2..4].join("-")
          file_date_str == target_date.strftime("%Y-%m-%d")
        when "fundamentals"
          # fundamentals/<provider>/<YYYY>/<MM>/<DD>.ndjson
          return false if parts.length < 5
          return false if provider && !provider.empty? && parts[1] != provider

          dd = File.basename(parts[4], ".ndjson")
          file_date_str = [parts[2], parts[3], dd].join("-")
          file_date_str == target_date.strftime("%Y-%m-%d")
        when "economic_events"
          # economic_events/<source>/<category>/<YYYY>/<MM>/<DD>.ndjson
          return false if parts.length < 6
          return false if provider && !provider.empty? && parts[1] != provider

          dd = File.basename(parts[5], ".ndjson")
          file_date_str = [parts[3], parts[4], dd].join("-")
          file_date_str == target_date.strftime("%Y-%m-%d")
        else
          false
        end
      end

      private

      def purge_remote_dataset(dataset_type, target_date, provider:, dry_run:, summary:)
        prefix = build_prefix(dataset_type, target_date, provider: provider)
        all_candidate_keys = @store.list_keys(prefix: prefix)

        matching_keys = all_candidate_keys.select do |key|
          matches_criteria?(key, dataset_type, target_date, provider: provider)
        end

        summary[:remote_keys_scanned] += matching_keys.size
        @runtime.logger.info("staging_purger: found #{matching_keys.size} remote key(s) in '#{@store.bucket}' for #{dataset_type} on #{target_date}")

        if matching_keys.any?
          if dry_run
            @runtime.logger.info("staging_purger: [DRY RUN] would delete #{matching_keys.size} remote key(s)")
          else
            @store.delete_keys(matching_keys)
            summary[:remote_keys_deleted] += matching_keys.size
            @runtime.logger.info("staging_purger: deleted #{matching_keys.size} remote key(s) from '#{@store.bucket}'")
          end
        end
      end

      def purge_local_dataset(dataset_type, target_date, provider:, dry_run:, summary:)
        dataset_dir = File.join(@data_dir, dataset_type)
        return unless Dir.exist?(dataset_dir)

        pattern = File.join(dataset_dir, StagingPublisher::DATASET_PATTERNS.fetch(dataset_type, "**/*"))
        local_files = Dir.glob(pattern).select { |path| File.file?(path) }

        matching_files = local_files.select do |file_path|
          rel = Pathname.new(file_path).relative_path_from(Pathname.new(@data_dir)).to_s
          matches_criteria?(rel, dataset_type, target_date, provider: provider)
        end

        summary[:local_files_scanned] += matching_files.size
        @runtime.logger.info("staging_purger: found #{matching_files.size} local file(s) for #{dataset_type} on #{target_date}")

        if matching_files.any?
          if dry_run
            @runtime.logger.info("staging_purger: [DRY RUN] would delete #{matching_files.size} local file(s)")
          else
            matching_files.each do |file_path|
              File.unlink(file_path) rescue nil
            end
            summary[:local_files_deleted] += matching_files.size
            @runtime.logger.info("staging_purger: deleted #{matching_files.size} local file(s)")
          end
        end
      end

      def normalize_datasets(dataset_types)
        return VALID_DATASETS if dataset_types.nil? || dataset_types.empty? || dataset_types == ["all"]

        invalid = dataset_types - VALID_DATASETS
        raise ArgumentError, "Unknown dataset type(s): #{invalid.join(', ')}. Valid: #{VALID_DATASETS.join(', ')}" if invalid.any?

        dataset_types
      end
    end
  end
end
