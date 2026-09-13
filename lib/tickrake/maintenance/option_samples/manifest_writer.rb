# frozen_string_literal: true

module Tickrake
  module Maintenance
    module OptionSamples
      class ManifestWriter
        SCHEMA_VERSION = 1

        def initialize(s3_archive:)
          @s3_archive = s3_archive
        end

        def write(dataset_type:, provider:, root:, sample_date:, artifacts:, archived_at:)
          key = manifest_key(dataset_type: dataset_type, provider: provider, root: root, sample_date: sample_date)
          payload = {
            schema_version: SCHEMA_VERSION,
            dataset_type: dataset_type,
            provider: provider,
            root: root,
            sample_date: sample_date.to_s,
            archived_at: archived_at.iso8601,
            artifacts: artifacts
          }
          @s3_archive.upload_content(key, JSON.generate(payload))
          "s3://#{@s3_archive.bucket}/#{key}"
        end

        def manifest_exists?(dataset_type:, provider:, root:, sample_date:)
          key = manifest_key(dataset_type: dataset_type, provider: provider, root: root, sample_date: sample_date)
          @s3_archive.list_keys(prefix: key).include?(key)
        end

        def read(dataset_type:, provider:, root:, sample_date:)
          key = manifest_key(dataset_type: dataset_type, provider: provider, root: root, sample_date: sample_date)
          content = @s3_archive.download_content(key)
          JSON.parse(content)
        end

        private

        def manifest_key(dataset_type:, provider:, root:, sample_date:)
          "manifests/#{dataset_type}/#{provider}/#{root}_#{sample_date}.json"
        end
      end
    end
  end
end
