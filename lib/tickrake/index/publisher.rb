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

      # DEPRECATED: publish() previously wrote ROOT.json and tickers.json.
      # Index building is now handled by the reconciler. This method is a no-op stub.
      def publish(provider:, root:)
      end
    end
  end
end
