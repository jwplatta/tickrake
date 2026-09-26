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

    # Order book events carry two timestamps:
    # - received_at: local collector receipt time (ms since epoch UTC). Safest cutoff for
    #   "what data did I have by time T?".
    # - book_time_ms: market snapshot timestamp (ms since epoch UTC) from the broker/exchange feed.
    def handle_event(event, service:)
      received_at = (Time.now.to_f * 1000).to_i
      entries = Array(event["content"] || event[:content] || [event])

      entries.each do |entry|
        symbol = entry["key"] || entry[:key]
        next unless symbol

        @events_writer.write(
          "job_type"     => "order_book",
          "provider"     => @provider,
          "symbol"       => symbol.to_s,
          "service"      => service,
          "received_at"  => received_at,
          "book_time_ms" => (entry["1"] || entry[:book_time])&.to_i,
          "bids_json"    => extract_json(entry["2"] || entry[:bids]),
          "asks_json"    => extract_json(entry["3"] || entry[:asks])
        )
      end
    end

    def extract_json(data)
      JSON.dump(data) if data
    end

    def log_prefix
      "[order_book:#{@job_name}]"
    end
  end
end
