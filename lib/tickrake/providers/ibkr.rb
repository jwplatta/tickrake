# frozen_string_literal: true

module Tickrake
  module Providers
    class Ibkr < Base
      INDEX_CONTRACTS = {
        "SPX" => { symbol: "SPX", exchange: "CBOE" },
        "VIX" => { symbol: "VIX", exchange: "CBOE" },
        "VIX9D" => { symbol: "VIX9D", exchange: "CBOE" },
        "VIX1D" => { symbol: "VIX1D", exchange: "CBOE" },
        "XSP" => { symbol: "XSP", exchange: "CBOE" },
        "NDX" => { symbol: "NDX", exchange: "NASDAQ" },
        "RUT" => { symbol: "RUT", exchange: "RUSSELL" }
      }.freeze
      FREQUENCY_CONFIG = {
        "1min" => { bar_size: "1 min", chunk_days: 6 },
        "5min" => { bar_size: "5 mins", chunk_days: 6 },
        "10min" => { bar_size: "10 mins", chunk_days: 6 },
        "15min" => { bar_size: "15 mins", chunk_days: 20 },
        "30min" => { bar_size: "30 mins", chunk_days: 34 },
        "day" => { bar_size: "1 day", chunk_days: 365 },
        "week" => { bar_size: "1 week", chunk_days: 365 },
        "month" => { bar_size: "1 month", chunk_days: 365 }
      }.freeze

      def initialize(provider_name:, settings:, connection_builder: nil)
        super(provider_name: provider_name, adapter_name: "ibkr")
        @settings = settings
        @connection_builder = connection_builder
      end

      def fetch_bars(symbol:, frequency:, start_date:, end_date:, extended_hours:, previous_close:)
        _ = previous_close
        config = FREQUENCY_CONFIG.fetch(frequency)
        contract = build_contract(symbol)

        with_connection do |connection|
          request_id = 1
          chunk_ranges(start_date, end_date, config.fetch(:chunk_days)).flat_map do |chunk_start, chunk_end|
            result = fetch_chunk(
              connection: connection,
              request_id: request_id,
              contract: contract,
              chunk_end: chunk_end,
              duration: duration_string(chunk_start, chunk_end),
              bar_size: config.fetch(:bar_size),
              extended_hours: extended_hours,
              requested_symbol: symbol,
              frequency: frequency
            )
            request_id += 1
            result
          end.sort_by(&:utc_datetime)
        end
      end

      private

      def with_connection
        connection = build_connection
        yield connection
      ensure
        connection&.disconnect
      end

      def build_connection
        return @connection_builder.call(**connection_settings) if @connection_builder

        load_ib_api!

        IB::Connection.new(
          host: connection_settings.fetch(:host),
          port: connection_settings.fetch(:port),
          client_id: connection_settings.fetch(:client_id),
          connect: true,
          received: true
        )
      end

      def connection_settings
        {
          host: @settings.fetch("host", "127.0.0.1"),
          port: Integer(@settings.fetch("port", 4002)),
          client_id: Integer(@settings.fetch("client_id", 1001))
        }
      end

      def fetch_chunk(connection:, request_id:, contract:, chunk_end:, duration:, bar_size:, extended_hours:, requested_symbol:, frequency:)
        load_ib_api!

        queue = Queue.new
        subscription = connection.subscribe(IB::Messages::Incoming::HistoricalData) do |message|
          queue << message if message.request_id == request_id
        end

        connection.send_message(
          IB::Messages::Outgoing::RequestHistoricalData.new(
            request_id: request_id,
            contract: contract,
            end_date_time: end_datetime_string(chunk_end),
            duration: duration,
            bar_size: bar_size,
            what_to_show: :trades,
            use_rth: extended_hours ? 0 : 1,
            keep_up_todate: false
          )
        )

        message = Timeout.timeout(Integer(@settings.fetch("historical_timeout_seconds", 30))) { queue.pop }
        Array(message.results).map do |bar|
          Data::Bar.new(
            datetime: normalize_time(bar.time),
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            volume: bar.volume,
            source: provider_name,
            symbol: requested_symbol,
            frequency: frequency
          )
        end
      ensure
        connection.unsubscribe(subscription) if subscription
      end

      def build_contract(symbol)
        load_ib_api!

        normalized = symbol.to_s.delete_prefix("$").upcase
        if INDEX_CONTRACTS.key?(normalized)
          config = INDEX_CONTRACTS.fetch(normalized)
          IB::Index.new(symbol: config.fetch(:symbol), exchange: config.fetch(:exchange), currency: "USD")
        else
          IB::Stock.new(symbol: normalized, currency: "USD", exchange: "SMART")
        end
      end

      def chunk_ranges(start_date, end_date, chunk_days)
        ranges = []
        cursor = start_date
        while cursor <= end_date
          chunk_end = [cursor + (chunk_days - 1), end_date].min
          ranges << [cursor, chunk_end]
          cursor = chunk_end + 1
        end
        ranges
      end

      def duration_string(start_date, end_date)
        "#{(end_date - start_date).to_i + 1} D"
      end

      def end_datetime_string(date)
        "#{date.strftime("%Y%m%d")} 23:59:59 UTC"
      end

      def normalize_time(value)
        case value
        when Time
          value.utc
        else
          Time.parse(value.to_s).utc
        end
      end

      def load_ib_api!
        return if defined?(IB)

        require "ib-api"
      end
    end
  end
end
