# frozen_string_literal: true

require "monitor"
require "fileutils"
require "json"

module Tickrake
  module Streaming
    class EventStreamWriter
      def initialize(config:, logger:, dataset_type:, provider:, provider_definition: nil)
        @config = config
        @logger = logger
        @dataset_type = dataset_type.to_s
        @provider = provider.to_s
        @provider_definition = provider_definition
        @paths = Tickrake::Storage::Paths.new(config)
        @lock = Monitor.new
        @open_files = {} # [date_str, symbol] => File
      end

      def write(event_hash)
        symbol = event_hash["symbol"]
        return unless symbol

        now = Time.now.utc
        date_str = now.strftime("%Y-%m-%d")

        @lock.synchronize do
          file = get_or_open_file(date_str, symbol.to_s, now)
          file.puts(JSON.generate(event_hash))
          file.flush
        end
      end

      def close
        @lock.synchronize do
          @open_files.each_value do |file|
            file.close rescue nil
          end
          @open_files.clear
        end
      end

      private

      def get_or_open_file(date_str, symbol, now)
        key = [date_str, symbol]
        return @open_files[key] if @open_files[key] && !@open_files[key].closed?

        # Close files from prior dates if date rolled over
        stale_keys = @open_files.keys.select { |d, _| d != date_str }
        stale_keys.each do |stale_key|
          @open_files[stale_key].close rescue nil
          @open_files.delete(stale_key)
        end

        target_path = resolve_target_path(symbol, now)
        FileUtils.mkdir_p(File.dirname(target_path))
        @open_files[key] = File.open(target_path, "a")
      end

      def resolve_target_path(symbol, timestamp)
        case @dataset_type
        when "level_one"
          @paths.level_one_path(
            provider: @provider,
            symbol: symbol,
            flush_start: timestamp,
            provider_definition: @provider_definition
          )
        when "order_book"
          @paths.order_book_path(
            provider: @provider,
            symbol: symbol,
            flush_start: timestamp,
            provider_definition: @provider_definition
          )
        else
          raise Tickrake::Error, "Unsupported streaming dataset type: #{@dataset_type}"
        end
      end
    end
  end
end
