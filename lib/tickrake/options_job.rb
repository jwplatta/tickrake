# frozen_string_literal: true

module Tickrake
  class OptionsJob
    def self.option_chain_api_symbol(symbol)
      SchwabRb::PriceHistory::Downloader.api_symbol(symbol)
    end

    def initialize(runtime, universe: nil, expiration_date: nil, progress_reporter: nil)
      @runtime = runtime
      @universe = universe
      @expiration_date = expiration_date
      @progress_reporter = progress_reporter
    end

    def run(now: Time.now)
      @runtime.logger.info("Starting options scrape at #{now.utc.iso8601}")
      run_time = now
      queue = build_queue(run_time.to_date)
      @runtime.logger.info("Resolved #{queue.length} option fetch tasks.")
      process_queue(queue, run_time)
      @progress_reporter&.finish
      @runtime.logger.info("Completed options scrape at #{Time.now.utc.iso8601}")
    end

    private

    def build_queue(base_date)
      return build_direct_queue(base_date) if @expiration_date

      buckets = @runtime.config.dte_buckets.uniq.sort

      selected_universe.flat_map do |entry|
        provider_name = provider_name_for(entry)
        expiration_chain = fetch_expiration_chain(client, entry.symbol)

        buckets.filter_map do |bucket|
          expiration = resolve_expiration(expiration_chain, bucket, entry.option_root)
          next unless expiration

          resolved_expiration = expiration.date_object || (base_date + bucket)
          @runtime.logger.info(
            "Resolved option expiration for #{entry.symbol} bucket=#{bucket} root=#{entry.option_root || '-'} to #{resolved_expiration}"
          )

          {
            symbol: entry.symbol,
            option_root: entry.option_root,
            provider_name: provider_name,
            expiration_date: resolved_expiration,
            requested_buckets: [bucket]
          }
        end
      end
    end

    def build_direct_queue(base_date)
      requested_bucket = (@expiration_date - base_date).to_i

      selected_universe.map do |entry|
        {
          symbol: entry.symbol,
          option_root: entry.option_root,
          provider_name: provider_name_for(entry),
          expiration_date: @expiration_date,
          requested_buckets: [requested_bucket]
        }
      end
    end

    def fetch_expiration_chain(client, symbol)
      chain = client.get_option_expiration_chain(symbol)
      raise Tickrake::Error, "Option expiration chain request returned nil for #{symbol}" if chain.nil?
      raise Tickrake::Error, "Unexpected option expiration chain result for #{symbol}: #{chain.class}" unless chain.respond_to?(:expiration_list)

      chain
    end

    def resolve_expiration(expiration_chain, bucket, option_root)
      expiration = Array(expiration_chain.expiration_list).find do |candidate|
        candidate.days_to_expiration == bucket && root_matches?(candidate, option_root)
      end

      return expiration if expiration

      @runtime.logger.info(
        "Skipping option fetch bucket=#{bucket} root=#{option_root || '-'} because Schwab reported no matching expiration."
      )
      nil
    end

    def root_matches?(expiration, option_root)
      return true if option_root.nil? || option_root.empty?

      expiration_roots(expiration).include?(option_root)
    end

    def expiration_roots(expiration)
      roots = expiration.respond_to?(:option_roots) ? expiration.option_roots : nil

      case roots
      when Array
        roots.map(&:to_s)
      when String
        roots.split(",").map(&:strip)
      when nil
        []
      else
        Array(roots).map(&:to_s)
      end
    end

    def process_queue(queue, run_time)
      index = 0
      mutex = Mutex.new
      worker_count = [@runtime.config.max_workers, queue.length].min

      Array.new(worker_count) do
        Thread.new do
          loop do
            job = mutex.synchronize do
              current = queue[index]
              index += 1 if current
              current
            end
            break unless job

            fetch_one(job, run_time)
          end
        end
      end.each(&:join)
    end

    def selected_universe
      @universe || @runtime.config.options_universe
    end

    def fetch_one(job, run_time)
      requested_bucket = job.fetch(:requested_buckets).join(",")
      @runtime.logger.info(
        "Fetching option chain for #{job.fetch(:symbol)} provider=#{job.fetch(:provider_name)} bucket=#{requested_bucket} resolved_exp=#{job.fetch(:expiration_date)} root=#{job[:option_root] || '-'}"
      )
      id = @runtime.tracker.record_start(
        job_type: "options_monitor",
        dataset_type: "options",
        symbol: job.fetch(:symbol),
        option_root: job[:option_root],
        requested_buckets: job.fetch(:requested_buckets),
        resolved_expiration: job.fetch(:expiration_date).iso8601,
        scheduled_for: run_time,
        started_at: Time.now
      )

      retries = 0
      begin
        result = Timeout.timeout(@runtime.config.option_fetch_timeout_seconds) do
          write_option_chain(
            client: client,
            provider_name: job.fetch(:provider_name),
            symbol: job.fetch(:symbol),
            expiration_date: job.fetch(:expiration_date),
            timestamp: run_time,
            root: job[:option_root]
          )
        end
        path = result.fetch(:path)
        @runtime.logger.info("Wrote option chain for #{job.fetch(:symbol)} to #{path}")
        @runtime.tracker.record_finish(id: id, status: "success", finished_at: Time.now, output_path: path)
        @progress_reporter&.advance(title: option_progress_title(job))
      rescue StandardError => e
        retries += 1
        if retries <= @runtime.config.retry_count
          @runtime.logger.warn("Retry #{retries} for #{job.fetch(:symbol)} exp=#{job.fetch(:expiration_date)}: #{e.message}")
          sleep @runtime.config.retry_delay_seconds
          retry
        end
        @runtime.logger.error("Failed option fetch for #{job.fetch(:symbol)} exp=#{job.fetch(:expiration_date)}: #{e.message}")
        @runtime.tracker.record_finish(id: id, status: "failed", finished_at: Time.now, error_message: e.message)
        @progress_reporter&.advance(title: "#{option_progress_title(job)} failed")
      end
    end

    def option_progress_title(job)
      [job.fetch(:symbol), job[:option_root], job.fetch(:expiration_date).iso8601].compact.reject(&:empty?).join(" ")
    end

    def write_option_chain(client:, provider_name:, symbol:, expiration_date:, timestamp:, root:)
      chain = client.get_option_chain(
        self.class.option_chain_api_symbol(symbol),
        contract_type: SchwabRb::Option::ContractTypes::ALL,
        strike_range: SchwabRb::Option::StrikeRanges::ALL,
        from_date: expiration_date,
        to_date: expiration_date
      )
      raise Tickrake::Error, "Option chain request returned nil for #{symbol} exp=#{expiration_date}" if chain.nil?

      rows = option_sample_rows(chain, root, provider_name)
      path = storage_paths.option_sample_path(
        provider: provider_name,
        symbol: symbol,
        expiration_date: expiration_date,
        timestamp: timestamp,
        root: root
      )
      option_sample_writer.write(path: path, rows: rows)

      { path: path, row_count: rows.length }
    end

    def option_sample_rows(chain, option_root, provider_name)
      filtered_options(chain, option_root).sort_by do |option|
        [
          option.expiration_date&.iso8601.to_s,
          option.put_call.to_s,
          option.strike.to_f
        ]
      end.map do |option|
        Tickrake::Data::OptionSampleRow.new(
          contract_type: option.put_call,
          symbol: option.symbol,
          description: option.description,
          strike: option.strike,
          expiration_date: option.expiration_date&.iso8601,
          mark: option.mark,
          bid: option.bid,
          bid_size: option.bid_size,
          ask: option.ask,
          ask_size: option.ask_size,
          last: option.last,
          last_size: option.last_size,
          open_interest: option.open_interest,
          total_volume: option.total_volume,
          delta: option.delta,
          gamma: option.gamma,
          theta: option.theta,
          vega: option.vega,
          rho: option.rho,
          volatility: option.volatility,
          theoretical_volatility: option.theoretical_volatility,
          theoretical_option_value: option.theoretical_option_value,
          intrinsic_value: option.intrinsic_value,
          extrinsic_value: option.extrinsic_value,
          underlying_price: chain.underlying_price,
          source: provider_name,
          fetched_at: Time.now.utc
        )
      end
    end

    def filtered_options(chain, option_root)
      options = Array(chain.call_opts) + Array(chain.put_opts)
      return options if option_root.nil? || option_root.empty?

      options.select { |option| option.option_root.to_s.upcase == option_root.to_s.upcase }
    end

    def storage_paths
      @storage_paths ||= Storage::Paths.new(@runtime.config)
    end

    def option_sample_writer
      @option_sample_writer ||= Storage::OptionSampleWriter.new
    end

    def provider_name_for(entry)
      provider_name = @runtime.config.provider_name_with_override(@runtime.provider_name, entry)
      provider_definition = @runtime.config.provider_definition(provider_name)
      unless provider_definition.adapter == "schwab"
        raise Tickrake::Error, "OptionsJob currently supports provider=schwab only (got #{provider_name})."
      end

      provider_name
    end

    def client
      @client ||= @runtime.client_factory.build
    end
  end
end
