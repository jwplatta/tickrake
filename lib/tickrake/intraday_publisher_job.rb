# frozen_string_literal: true

module Tickrake
  class IntradayPublisherJob
    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
      @last_cleared_date = nil
    end

    def run(now: Time.now)
      datastore_name = @scheduled_job.settings.fetch("datastore_name")
      datastore_config = @runtime.config.datastore(datastore_name)
      store = Storage::S3Archive.new(@runtime.config, datastore_config: datastore_config)

      check_daily_clear(store, now)

      options_indexes = publish_options(store, datastore_config)
      candle_indexes = publish_candles(store, datastore_config)

      publish_unified_indexes(store, datastore_config, options_indexes, candle_indexes)
    end

    private

    def check_daily_clear(store, now)
      clear_at = @scheduled_job.settings["clear_at"]
      return unless clear_at

      target_hour, target_min = clear_at.split(":").map { |p| Integer(p, 10) }
      today_str = now.strftime("%Y-%m-%d")

      current_minutes = (now.hour * 60) + now.min
      target_minutes = (target_hour * 60) + target_min

      if current_minutes >= target_minutes && @last_cleared_date != today_str
        keys_to_clear = store.list_keys(prefix: "intraday/")
        unless keys_to_clear.empty?
          store.delete_keys(keys_to_clear)
          @runtime.logger.info("intraday_publisher: cleared #{keys_to_clear.size} key(s) from intraday store at #{clear_at}")
        end
        @last_cleared_date = today_str
      end
    end

    def publish_options(store, datastore_config)
      active_roots = @runtime.tracker.intraday_active_roots
      return {} if active_roots.empty?

      indexes = {}

      active_roots.group_by { |r| r[:provider_name] }.each do |provider_name, pairs|
        pairs.each do |pair|
          root = pair[:root]
          latest_rows = @runtime.tracker.intraday_index_rows(provider_name: provider_name, root: root)
          series_rows = @runtime.tracker.intraday_series_rows(provider_name: provider_name, root: root)
          next if latest_rows.empty? && series_rows.empty?

          sample_date = (series_rows.first || latest_rows.first).fetch("sample_date")

          existing_series_keys = store.list_keys(prefix: "intraday/#{provider_name}/options/#{sample_date}/#{root}_exp")
          series_files = series_rows.map do |row|
            expiration = row.fetch("expiration_date")
            sampled_at = row.fetch("sampled_at")
            filename = File.basename(row.fetch("path"))
            csv_key = "intraday/#{provider_name}/options/#{sample_date}/#{filename}"
            remote_uri = "s3://#{datastore_config.bucket}/#{csv_key}"

            unless existing_series_keys.include?(csv_key)
              store.upload_file(row.fetch("path"), key: csv_key)
              @runtime.logger.info("intraday_publisher: uploaded series #{row.fetch("path")} → #{remote_uri} (#{row.fetch("row_count")} rows)")
            end

            {
              "expiration_date" => expiration,
              "sampled_at" => sampled_at,
              "format" => "csv",
              "uri" => remote_uri,
              "row_count" => row.fetch("row_count")
            }
          end

          uploaded_latest_keys = []
          latest_files = latest_rows.map do |row|
            expiration = row.fetch("expiration_date")
            csv_key = "intraday/#{provider_name}/options/latest/#{root}_exp#{expiration}.csv"
            store.upload_file(row.fetch("path"), key: csv_key)
            uploaded_latest_keys << csv_key
            remote_uri = "s3://#{datastore_config.bucket}/#{csv_key}"
            @runtime.logger.info("intraday_publisher: uploaded latest #{row.fetch("path")} → #{remote_uri} (#{row.fetch("row_count")} rows)")
            {
              "expiration_date" => expiration,
              "format" => "csv",
              "uri" => remote_uri,
              "row_count" => row.fetch("row_count")
            }
          end

          stale_latest_keys = store.list_keys(prefix: "intraday/#{provider_name}/options/latest/#{root}_exp") - uploaded_latest_keys
          unless stale_latest_keys.empty?
            store.delete_keys(stale_latest_keys)
            @runtime.logger.info("intraday_publisher: evicted #{stale_latest_keys.size} stale option key(s): #{stale_latest_keys.join(", ")}")
          end

          first_latest = latest_rows.first || series_rows.last
          key = [provider_name, root]
          indexes[key] = {
            "sample_date" => sample_date,
            "status" => "complete",
            "latest" => {
              "sampled_at" => first_latest.fetch("sampled_at"),
              "files" => latest_files
            },
            "series" => series_files
          }
        end
      end

      indexes
    end

    def publish_candles(store, datastore_config)
      candles_dir = @runtime.config.candles_dir
      return {} unless candles_dir && Dir.exist?(candles_dir)

      indexes = {}

      Dir.glob(File.join(candles_dir, "*")).each do |provider_dir|
        next unless File.directory?(provider_dir)

        provider_name = File.basename(provider_dir)
        uploaded_keys = []

        Dir.glob(File.join(provider_dir, "**", "*.csv")).each do |csv_path|
          relative = csv_path.delete_prefix("#{provider_dir}/")
          parts = relative.split("/")
          next unless parts.size == 2

          frequency = parts[0]
          symbol = File.basename(parts[1], ".csv")
          row_count = File.readlines(csv_path).size - 1
          next if row_count <= 0

          csv_key = "intraday/#{provider_name}/candles/#{frequency}/#{symbol}.csv"
          store.upload_file(csv_path, key: csv_key)
          uploaded_keys << csv_key
          remote_uri = "s3://#{datastore_config.bucket}/#{csv_key}"
          @runtime.logger.info("intraday_publisher: uploaded #{csv_path} → #{remote_uri} (#{row_count} rows)")

          key = [provider_name, symbol]
          indexes[key] ||= { "files" => [] }
          indexes[key]["files"] << {
            "frequency" => frequency,
            "format" => "csv",
            "uri" => remote_uri,
            "row_count" => row_count
          }
        end

        stale_keys = store.list_keys(prefix: "intraday/#{provider_name}/candles/") - uploaded_keys
        unless stale_keys.empty?
          store.delete_keys(stale_keys)
          @runtime.logger.info("intraday_publisher: evicted #{stale_keys.size} stale candle key(s): #{stale_keys.join(", ")}")
        end
      end

      indexes
    end

    def publish_unified_indexes(store, datastore_config, options_indexes, candle_indexes)
      all_keys = (options_indexes.keys + candle_indexes.keys).uniq
      return if all_keys.empty?

      published_count = 0
      all_keys.each do |provider_name, root|
        index = {
          "provider" => provider_name,
          "root" => root,
          "updated_at" => Time.now.utc.iso8601
        }

        index["option_chains"] = options_indexes[[provider_name, root]] if options_indexes.key?([provider_name, root])
        index["candles"] = candle_indexes[[provider_name, root]] if candle_indexes.key?([provider_name, root])

        index_key = "intraday/#{provider_name}/#{root}.json"
        store.upload_content(index_key, JSON.generate(index))
        @runtime.logger.info("intraday_publisher: published index s3://#{datastore_config.bucket}/#{index_key}")
        published_count += 1
      end

      @runtime.logger.info("intraday_publisher: published #{published_count} root index(es)")
    end
  end
end
