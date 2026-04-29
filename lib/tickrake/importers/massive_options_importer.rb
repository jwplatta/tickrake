# frozen_string_literal: true

module Tickrake
  module Importers
    class MassiveOptionsImporter
      Result = Struct.new(:path, :row_count, :expiration_date, :sample_datetime, keyword_init: true)
      Target = Struct.new(:path, :tmp_path, :option_root, :expiration_date, :sample_datetime, :row_count, keyword_init: true)

      def initialize(config:, tracker:, provider_name:, ticker:, option_root:, source_path:, force: false, logger: nil)
        @config = config
        @tracker = tracker
        @provider_name = provider_name.to_s
        @option_root = option_root.to_s.upcase
        @ticker = ticker.to_s.empty? ? config.ticker_for_option_root(@option_root) : ticker.to_s.upcase
        @source_path = Tickrake::PathSupport.expand_path(source_path)
        @force = force
        @logger = logger || Logger.new(nil)
        @storage_paths = Tickrake::Storage::Paths.new(config)
        @symbol_parser = MassiveOptionSymbol.new
        @targets = {}
      end

      def import
        validate!
        Dir.mktmpdir("tickrake-massive-options-import-") do |tmp_dir|
          @tmp_dir = tmp_dir
          stream_source
          move_targets
        end
      ensure
        @tmp_dir = nil
        @targets = {}
      end

      private

      def validate!
        raise Tickrake::Error, "Import source does not exist: #{@source_path}" unless File.file?(@source_path)

        provider = @config.provider_definition(@provider_name)
        raise Tickrake::Error, "Provider `#{@provider_name}` must use adapter massive." unless provider.adapter == "massive"

        expected_ticker = @config.ticker_for_option_root(@option_root)
        return if expected_ticker.casecmp?(@ticker)

        raise Tickrake::Error,
              "Option root #{@option_root} maps to ticker #{expected_ticker}, but import requested ticker #{@ticker}."
      end

      def stream_source
        CSV.foreach(@source_path, headers: true) do |row|
          parsed = parse_symbol(row.fetch("ticker"))
          next unless parsed.massive_root.casecmp?(@option_root)

          sample_time = parse_window_start(row.fetch("window_start"))
          target = target_for(parsed, sample_time)
          append_row(target, option_sample_row(parsed, row, sample_time))
        end
      end

      def parse_symbol(ticker)
        @symbol_parser.parse(ticker)
      rescue Tickrake::Error
        raise
      end

      def parse_window_start(value)
        nanoseconds = Integer(value, 10)
        seconds = nanoseconds / 1_000_000_000
        remainder = nanoseconds % 1_000_000_000
        Time.at(seconds, remainder, :nanosecond).utc
      rescue ArgumentError
        raise Tickrake::Error, "Invalid Massive window_start `#{value}`."
      end

      def target_for(parsed, sample_time)
        key = [@option_root, parsed.expiration_date.iso8601, sample_time.to_i]
        @targets[key] ||= begin
          path = @storage_paths.option_sample_path(
            provider: @provider_name,
            symbol: @ticker,
            expiration_date: parsed.expiration_date,
            timestamp: sample_time,
            root: @option_root
          )
          if File.exist?(path) && !@force
            raise Tickrake::Error, "Import target already exists: #{path}. Use --force to replace it."
          end

          Target.new(
            path: path,
            tmp_path: File.join(@tmp_dir, "#{Digest::SHA256.hexdigest(path)}.csv"),
            option_root: @option_root,
            expiration_date: parsed.expiration_date,
            sample_datetime: sample_time,
            row_count: 0
          )
        end
      end

      def append_row(target, row)
        write_header = target.row_count.zero?
        CSV.open(target.tmp_path, "ab") do |csv|
          csv << Tickrake::Storage::OptionSampleWriter::CSV_HEADERS if write_header
          csv << Tickrake::Storage::OptionSampleWriter.csv_row(row)
        end
        target.row_count += 1
      end

      def option_sample_row(parsed, row, sample_time)
        Tickrake::Data::OptionSampleRow.new(
          contract_type: parsed.contract_type,
          symbol: parsed.symbol,
          description: parsed.description,
          strike: parsed.strike,
          expiration_date: parsed.expiration_date.iso8601,
          open: row["open"],
          high: row["high"],
          low: row["low"],
          close: row["close"],
          total_volume: row["volume"],
          transactions: row["transactions"],
          source: @provider_name,
          fetched_at: sample_time
        )
      end

      def move_targets
        metadata_batch = []
        results = @targets.values.map do |target|
          next if target.row_count.zero?

          FileUtils.mkdir_p(File.dirname(target.path))
          FileUtils.mv(target.tmp_path, target.path, force: @force)
          metadata_batch << metadata_attributes_for(target)
          @logger.info("Imported #{target.row_count} Massive option rows to #{target.path}")
          Result.new(
            path: target.path,
            row_count: target.row_count,
            expiration_date: target.expiration_date,
            sample_datetime: target.sample_datetime
          )
        end.compact

        unless metadata_batch.empty?
          @tracker.bulk_upsert_file_metadata(metadata_batch)
          @logger.info("Committed #{metadata_batch.length} metadata cache rows for Massive import #{@source_path}")
        end

        results
      end

      def metadata_attributes_for(target)
        stat = File.stat(target.path)
        observed_at = target.sample_datetime.utc.iso8601
        {
          path: target.path,
          dataset_type: "options",
          provider_name: @provider_name,
          ticker: target.option_root,
          frequency: nil,
          expiration_date: target.expiration_date.iso8601,
          row_count: target.row_count,
          first_observed_at: observed_at,
          last_observed_at: observed_at,
          file_mtime: stat.mtime.to_i,
          file_size: stat.size,
          updated_at: Time.now
        }
      end
    end
  end
end
