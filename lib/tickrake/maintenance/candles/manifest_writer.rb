# frozen_string_literal: true

module Tickrake
  module Maintenance
    module Candles
      class ManifestWriter
        def initialize(s3_archive:)
          @s3_archive = s3_archive
        end

        def write(provider:, symbol:, frequency:, years_data:)
          existing = read(provider: provider, symbol: symbol)
          frequencies = existing.fetch("frequencies", {})

          frequencies[frequency] = {
            "years" => years_data.map do |yd|
              {
                "year" => yd[:year],
                "row_count" => yd[:row_count],
                "first_datetime" => yd[:first_datetime],
                "last_datetime" => yd[:last_datetime],
                "uri" => yd[:uri]
              }
            end
          }

          manifest = {
            "provider" => provider,
            "symbol" => symbol,
            "updated_at" => Time.now.utc.iso8601,
            "frequencies" => frequencies
          }

          key = manifest_key(provider, symbol)
          @s3_archive.upload_content(key, JSON.generate(manifest))
          "s3://#{@s3_archive.bucket}/#{key}"
        end

        def read(provider:, symbol:)
          key = manifest_key(provider, symbol)
          return {} unless @s3_archive.object_exists?(key)

          JSON.parse(@s3_archive.download_content(key))
        rescue StandardError
          {}
        end

        private

        def manifest_key(provider, symbol)
          "manifests/candles/#{provider}/#{symbol}.json"
        end
      end
    end
  end
end
