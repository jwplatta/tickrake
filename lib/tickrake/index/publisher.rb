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

      # Writes ROOT.json with the intraday section for the given provider/root.
      # Historical section is now built by the reconciler from S3 manifests.
      def publish(provider:, root:)
        write_root_index(provider, root)
      end

      private

      def write_root_index(provider, root)
        builder = RootIndexBuilder.new(tracker: @tracker, options_dir: @options_dir)
        payload = builder.build(provider: provider, root: root)
        path = root_index_path(provider, root)
        WriteLock.new(provider: provider, root: root).synchronize do
          AtomicJsonWriter.new.write(path, payload)
        end
        upload_to_s3(path) if @s3_archive
      end

      def root_index_path(provider, root)
        File.join(@options_dir, provider.to_s, "#{root}.json")
      end

      def upload_to_s3(path)
        @s3_archive.upload(path)
      rescue => e
        @logger&.warn("publisher s3 upload failed path=#{path} error=#{e.message}")
      end
    end
  end
end
