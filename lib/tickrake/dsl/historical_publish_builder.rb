# frozen_string_literal: true

module Tickrake
  module DSL
    class HistoricalPublishBuilder
      VALID_DATASETS = %w[options candles level_one order_book fundamentals economic_events].freeze

      def initialize
        @datastore_name = nil
        @datasets = nil
        @batch_size = nil
        @delete_after_upload = false
        @retain_days = nil
      end

      def datastore(name)
        @datastore_name = name.to_s
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

      def retain_days(n)
        @retain_days = Integer(n)
      end

      def build!
        raise Tickrake::Error, "historical_publish block requires datastore" if @datastore_name.nil?

        if @datasets
          invalid = @datasets - VALID_DATASETS
          raise Tickrake::Error, "Unknown dataset(s) in historical_publish: #{invalid.join(', ')}" if invalid.any?
        end

        {
          "datastore_name" => @datastore_name,
          "datasets" => @datasets,
          "batch_size" => @batch_size,
          "delete_after_upload" => @delete_after_upload,
          "retain_days" => @retain_days
        }.compact
      end
    end
  end
end
