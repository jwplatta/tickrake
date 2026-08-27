# frozen_string_literal: true

module Tickrake
  module Index
    class Publisher
      def initialize(tracker:, options_dir:, logger:, s3_archive: nil)
        @tracker = tracker
        @options_dir = options_dir
        @logger = logger
        @s3_archive = s3_archive
      end

      def publish(provider:, root:)
        WriteLock.new(provider: provider, root: root).synchronize do
          write_root_index(provider: provider, root: root)
          write_tickers_index(provider: provider)
        end
      end

      private

      def write_root_index(provider:, root:)
        payload = RootIndexBuilder.new(tracker: @tracker, options_dir: @options_dir)
          .build(provider: provider, root: root)
        path = root_index_path(provider, root)
        AtomicJsonWriter.new.write(path, payload)
        upload_to_s3(path)
        @logger&.info("index publish provider=#{provider} root=#{root} path=#{path}")
      end

      def write_tickers_index(provider:)
        payload = TickersIndexBuilder.new(tracker: @tracker).build(provider: provider)
        path = tickers_index_path(provider)
        AtomicJsonWriter.new.write(path, payload)
        upload_to_s3(path)
        @logger&.info("index publish tickers provider=#{provider} path=#{path}")
      end

      def root_index_path(provider, root)
        File.join(@options_dir, provider, "#{root}.json")
      end

      def tickers_index_path(provider)
        File.join(@options_dir, provider, "tickers.json")
      end

      def upload_to_s3(path)
        return unless @s3_archive

        @s3_archive.upload(path)
      end
    end
  end
end
