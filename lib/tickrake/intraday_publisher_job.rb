# frozen_string_literal: true

module Tickrake
  class IntradayPublisherJob
    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
    end

    def run
      datastore_name = @scheduled_job.settings.fetch("datastore_name")
      datastore_config = @runtime.config.datastore(datastore_name)
      store = Storage::S3Archive.new(@runtime.config, datastore_config: datastore_config)

      active_roots = @runtime.tracker.intraday_active_roots
      return if active_roots.empty?

      published_count = 0
      active_roots.group_by { |r| r[:provider_name] }.each do |provider_name, pairs|
        pairs.each do |pair|
          root = pair[:root]
          rows = @runtime.tracker.intraday_index_rows(provider_name: provider_name, root: root)
          next if rows.empty?

          uploaded_keys = []
          intraday_files = rows.map do |row|
            expiration = row.fetch("expiration_date")
            csv_key = "intraday/#{provider_name}/options/#{root}_exp#{expiration}.csv"
            store.upload_file(row.fetch("path"), key: csv_key)
            uploaded_keys << csv_key
            remote_uri = "s3://#{datastore_config.bucket}/#{csv_key}"
            @runtime.logger.info("intraday_publisher: uploaded #{row.fetch("path")} → #{remote_uri} (#{row.fetch("row_count")} rows)")
            {
              "expiration_date" => expiration,
              "format" => "csv",
              "uri" => remote_uri,
              "row_count" => row.fetch("row_count")
            }
          end

          stale_keys = store.list_keys(prefix: "intraday/#{provider_name}/options/#{root}_exp") - uploaded_keys
          unless stale_keys.empty?
            store.delete_keys(stale_keys)
            @runtime.logger.info("intraday_publisher: evicted #{stale_keys.size} stale key(s): #{stale_keys.join(", ")}")
          end

          first = rows.first
          intraday_index = {
            "schema_version" => 1,
            "provider" => provider_name,
            "root" => root,
            "updated_at" => Time.now.utc.iso8601,
            "intraday" => {
              "sample_date" => first.fetch("sample_date"),
              "sampled_at" => first.fetch("sampled_at"),
              "status" => "complete",
              "files" => intraday_files
            }
          }

          index_key = "intraday/#{provider_name}/#{root}.json"
          store.upload_content(index_key, JSON.generate(intraday_index))
          @runtime.logger.info("intraday_publisher: published index s3://#{datastore_config.bucket}/#{index_key} (#{intraday_files.size} expiration(s))")
          published_count += 1
        end
      end

      @runtime.logger.info("intraday_publisher: published #{published_count} root index(es)")
    end
  end
end
