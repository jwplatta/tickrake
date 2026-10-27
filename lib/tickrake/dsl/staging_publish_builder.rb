# frozen_string_literal: true

module Tickrake
  module DSL
    class StagingPublishBuilder
      VALID_DATASETS = %w[options candles level_one order_book fundamentals economic_events].freeze

      def initialize
        @datasets = nil
        @batch_size = nil
        @delete_after_upload = true
      end

      def datasets(*list)
        flattened = list.flatten.map(&:to_s)
        @datasets = flattened.include?("all") ? nil : flattened
      end

      def batch_size(n)
        @batch_size = Integer(n)
      end

      def delete_after_upload(val = true)
        @delete_after_upload = !!val
      end

      def build!
        if @datasets
          invalid = @datasets - VALID_DATASETS
          raise Tickrake::Error, "Unknown dataset(s) in staging_publish: #{invalid.join(', ')}" if invalid.any?
        end

        {
          "datasets" => @datasets,
          "batch_size" => @batch_size,
          "delete_after_upload" => @delete_after_upload
        }.compact
      end
    end
  end
end
