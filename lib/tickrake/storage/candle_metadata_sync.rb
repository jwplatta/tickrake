# frozen_string_literal: true

module Tickrake
  module Storage
    class CandleMetadataSync
      Result = Struct.new(
        :providers_scanned,
        :files_discovered,
        :rows_inserted,
        :files_skipped,
        keyword_init: true
      )

      def initialize(config:, tracker:, provider_name: nil, metadata_builder: nil)
        @config = config
        @tracker = tracker
        @provider_name = provider_name
        @metadata_builder = metadata_builder || Tickrake::Query::CandleMetadata.new(config: config)
      end

      def run
        providers = selected_providers
        files_discovered = 0
        rows_inserted = 0
        files_skipped = 0

        providers.each do |provider|
          candle_paths_for(provider).each do |path|
            files_discovered += 1
            if @tracker.file_metadata(path)
              files_skipped += 1
              next
            end

            metadata = @metadata_builder.build(path: path, provider_name: provider)
            if metadata
              @tracker.upsert_file_metadata(metadata)
              rows_inserted += 1
            else
              files_skipped += 1
            end
          end
        end

        Result.new(
          providers_scanned: providers,
          files_discovered: files_discovered,
          rows_inserted: rows_inserted,
          files_skipped: files_skipped
        )
      end

      private

      def selected_providers
        return [@config.provider_definition(@provider_name).name] if @provider_name

        @config.providers.keys.sort
      end

      def candle_paths_for(provider_name)
        base_dir = File.join(@config.history_dir, provider_name.to_s)
        return [] unless Dir.exist?(base_dir)

        Dir.glob(File.join(base_dir, "*.csv")).sort
      end
    end
  end
end
