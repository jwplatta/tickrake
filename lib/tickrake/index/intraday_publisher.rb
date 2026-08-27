# frozen_string_literal: true

module Tickrake
  module Index
    class IntradayPublisher
      def initialize(tracker:, options_dir:, logger:)
        @tracker = tracker
        @options_dir = options_dir
        @logger = logger
      end

      def publish(collection_id:, expected_counts:)
        expected_counts.each do |(provider, root), expected|
          received = @tracker.collection_file_count(
            collection_id: collection_id,
            provider_name: provider,
            root: root
          )

          if received >= expected && expected > 0
            @logger&.info(
              "intraday publish provider=#{provider} root=#{root} " \
              "collection_id=#{collection_id} expected=#{expected} received=#{received}"
            )
            Publisher.new(tracker: @tracker, options_dir: @options_dir, logger: @logger)
              .publish(provider: provider, root: root)
          else
            @logger&.warn(
              "intraday publish skipped provider=#{provider} root=#{root} " \
              "collection_id=#{collection_id} expected=#{expected} received=#{received}"
            )
          end
        end
      end
    end
  end
end
