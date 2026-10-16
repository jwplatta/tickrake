# frozen_string_literal: true

require "json"

module Tickrake
  module Storage
    class HistoricalManifestWriter
      MANIFEST_PREFIX = "manifests"

      def initialize(store)
        @store = store
      end

      def manifest_key(dataset_type)
        "#{MANIFEST_PREFIX}/#{dataset_type}.json"
      end

      def read(dataset_type)
        key = manifest_key(dataset_type)
        return empty_manifest(dataset_type) unless @store.object_exists?(key)

        raw = @store.download_content(key)
        JSON.parse(raw)
      rescue StandardError
        empty_manifest(dataset_type)
      end

      def upsert_entries(dataset_type, new_entries)
        return if new_entries.nil? || new_entries.empty?

        current = read(dataset_type)
        entries_by_key = (current["files"] || []).each_with_object({}) do |f, h|
          h[f["key"]] = f
        end

        new_entries.each do |entry|
          k = entry["key"] || entry[:key]
          payload = entry.transform_keys(&:to_s)
          entries_by_key[k] = payload
        end

        file_list = entries_by_key.values.sort_by { |f| f["key"].to_s }
        total_rows = file_list.sum { |f| f["row_count"].to_i }
        total_size = file_list.sum { |f| f["file_size"].to_i }

        manifest_data = {
          "dataset" => dataset_type.to_s,
          "updated_at" => Time.now.utc.iso8601,
          "file_count" => file_list.size,
          "total_rows" => total_rows,
          "total_size_bytes" => total_size,
          "files" => file_list
        }

        write(dataset_type, manifest_data)
        manifest_data
      end

      def write(dataset_type, manifest_data)
        key = manifest_key(dataset_type)
        json_content = JSON.pretty_generate(manifest_data)
        @store.upload_content(key, json_content)
      end

      private

      def empty_manifest(dataset_type)
        {
          "dataset" => dataset_type.to_s,
          "updated_at" => Time.now.utc.iso8601,
          "file_count" => 0,
          "total_rows" => 0,
          "total_size_bytes" => 0,
          "files" => []
        }
      end
    end
  end
end
