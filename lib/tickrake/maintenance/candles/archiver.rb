# frozen_string_literal: true

module Tickrake
  module Maintenance
    module Candles
      class Archiver
        def initialize(context:, s3_archive:, manifest_writer: nil)
          @context = context
          @s3_archive = s3_archive
          @manifest_writer = manifest_writer || ManifestWriter.new(s3_archive: s3_archive)
        end

        def upload(years:)
          artifact_results = []
          years_data = []

          years.each do |year|
            path = @context.compacted_path(year)
            unless File.exist?(path)
              return ArchiveResult.new(
                success: false,
                provider_name: @context.provider_name,
                symbol: @context.symbol,
                frequency: @context.frequency,
                artifact_results: artifact_results,
                errors: ["Parquet file not found: #{path}"]
              )
            end

            s3_key = "candles/#{@context.provider_name}/#{@context.frequency}/#{year}/#{@context.symbol}.parquet"
            @s3_archive.upload_file(path, key: s3_key)
            remote_uri = "s3://#{@s3_archive.bucket}/#{s3_key}"

            artifact_results << { path: path, remote_uri: remote_uri, year: year }

            rows = Tickrake::Storage::CandleParquetWriter.new.read(path)
            datetimes = rows.map { |r| r[0] }.compact.sort
            years_data << {
              year: year,
              row_count: rows.size,
              first_datetime: datetimes.first,
              last_datetime: datetimes.last,
              uri: remote_uri
            }
          end

          @manifest_writer.write(
            provider: @context.provider_name,
            symbol: @context.symbol,
            frequency: @context.frequency,
            years_data: years_data
          )

          @context.logger.info(
            "candle_archiver: archived #{@context.symbol} #{@context.frequency} " \
            "#{years.size} year file(s) to S3"
          )

          ArchiveResult.new(
            success: true,
            provider_name: @context.provider_name,
            symbol: @context.symbol,
            frequency: @context.frequency,
            artifact_results: artifact_results,
            errors: []
          )
        rescue StandardError => e
          ArchiveResult.new(
            success: false,
            provider_name: @context.provider_name,
            symbol: @context.symbol,
            frequency: @context.frequency,
            artifact_results: artifact_results,
            errors: ["#{e.class}: #{e.message}"]
          )
        end
      end
    end
  end
end
