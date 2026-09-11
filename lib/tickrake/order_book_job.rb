# frozen_string_literal: true

module Tickrake
  class OrderBookJob
    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
      @order_book_config = scheduled_job.settings
      @job_name = scheduled_job.name
      @provider = scheduled_job.provider

      @stop_requested = false
      @subscribed_symbols = []
      @last_resolved_at = nil

      @events_writer = Tickrake::EventsWriter.new(
        pending_events_dir: runtime.config.pending_events_dir,
        job_name: @job_name,
        rotation_interval_seconds: @order_book_config.rotation_interval_seconds,
        logger: runtime.logger
      )
    end

    def run_session(window_start:)
      @stop_requested = false
      @subscribed_symbols = []
      @last_resolved_at = nil

      @events_writer.recover_stale_files

      client = Tickrake::ClientFactory.new(@runtime.config).build
      stream = SchwabRb::Stream::Client.new(client)

      if @order_book_config.options_book?
        resolver = Tickrake::OrderBook::ContractResolver.new(client, @order_book_config.contracts)
        initial_symbols = resolver.resolve
        @subscribed_symbols = initial_symbols.dup
        @last_resolved_at = Time.now
        @runtime.logger.info("#{log_prefix} Resolved #{initial_symbols.size} initial contracts: #{initial_symbols.first(5).join(", ")}...")
      else
        @subscribed_symbols = @scheduled_job.universe.dup
      end

      @order_book_config.services.each do |service|
        stream.on(service.downcase.to_sym, symbols: @subscribed_symbols, fields: :all) do |event|
          handle_event(event, service: service)
        end
      end

      stream.start_async

      resolve_loop(stream: stream, resolver: @order_book_config.options_book? ? resolver : nil)
    ensure
      stream&.stop rescue nil
      @events_writer.close
    end

    def stop
      @stop_requested = true
    end

    def close; end

    private

    def resolve_loop(stream:, resolver:)
      until @stop_requested
        sleep(0.25)
        next unless resolver

        elapsed = Time.now - @last_resolved_at
        next unless elapsed >= @order_book_config.re_resolve_interval_seconds

        new_symbols = resolver.resolve
        added = new_symbols - @subscribed_symbols
        unless added.empty?
          @runtime.logger.info("#{log_prefix} Adding #{added.size} new contracts from re-resolution.")
          @order_book_config.services.each do |service|
            stream.on(service.downcase.to_sym, symbols: added, fields: :all) do |event|
              handle_event(event, service: service)
            end
          end
          @subscribed_symbols |= added
        end
        @last_resolved_at = Time.now
      end
    end

    def handle_event(event, service:)
      received_at = (Time.now.to_f * 1000).to_i
      entries = Array(event.try(:content) || event[:content] || event["content"] || [event])

      entries.each do |entry|
        symbol = entry.try(:key) || entry[:key] || entry["key"] || entry.try(:symbol) || entry[:symbol] || entry["symbol"]
        next unless symbol

        @events_writer.write(
          "job_type"     => "order_book",
          "provider"     => @provider,
          "symbol"       => symbol.to_s,
          "service"      => service,
          "received_at"  => received_at,
          "book_time_ms" => extract_book_time(entry),
          "bids_json"    => extract_bids_json(entry),
          "asks_json"    => extract_asks_json(entry)
        )
      end
    end

    def extract_book_time(entry)
      (entry.try(:book_time) || entry[:book_time] || entry["book_time"] ||
       entry.try(:timestamp) || entry[:timestamp] || entry["timestamp"])&.to_i
    end

    def extract_bids_json(entry)
      bids = entry.try(:bids) || entry[:bids] || entry["bids"]
      JSON.dump(bids) if bids
    end

    def extract_asks_json(entry)
      asks = entry.try(:asks) || entry[:asks] || entry["asks"]
      JSON.dump(asks) if asks
    end

    def log_prefix
      "[order_book:#{@job_name}]"
    end
  end
end
