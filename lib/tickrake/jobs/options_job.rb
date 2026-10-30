# frozen_string_literal: true

module Tickrake
  class OptionsJob
    def self.option_chain_api_symbol(symbol)
      SchwabRb::PriceHistory::Downloader.api_symbol(symbol)
    end

    def initialize(runtime, universe: nil, expiration_date: nil, progress_reporter: nil, scheduled_job: nil)
      @runtime = runtime
      @universe = universe
      @expiration_date = expiration_date
      @progress_reporter = progress_reporter
      @scheduled_job = scheduled_job
    end

    def run(now: Time.now)
      run_time = now
      collection_id = "options-#{run_time.utc.strftime("%Y%m%dT%H%M%SZ")}"
      @runtime.logger.info({ msg: "scrape_start", event: "scrape_start", collection_id: collection_id, scheduled_for: run_time.utc.iso8601 })
      queue = build_queue(run_time.to_date)
      @runtime.logger.info({ msg: "queue_resolved", event: "queue_resolved", task_count: queue.length, collection_id: collection_id })
      result = process_queue(queue, run_time, collection_id)
      @progress_reporter&.finish
      elapsed_ms = ((Time.now - run_time) * 1000).round
      @runtime.logger.info({ msg: "scrape_end", event: "scrape_end", collection_id: collection_id, success_count: result.success_count, failure_count: result.failure_count, duration_ms: elapsed_ms })
      result
    end

    private

    def build_queue(base_date)
      return build_direct_queue(base_date) if @expiration_date

      buckets = selected_dte_buckets.uniq.sort
      batch_size = selected_batch_size

      selected_universe.flat_map do |entry|
        provider_name = provider_name_for(entry)
        rate_limiter_for(provider_name)&.consume!
        expiration_chain = fetch_expiration_chain(client, entry.symbol)

        resolved_by_date = {}
        buckets.each do |bucket|
          expiration = resolve_expiration(expiration_chain, bucket, entry.option_root)
          next unless expiration

          resolved_expiration = expiration.date_object || (base_date + bucket)
          @runtime.logger.info(
            "Resolved option expiration for #{entry.symbol} bucket=#{bucket} root=#{entry.option_root || '-'} to #{resolved_expiration}"
          )

          item = (resolved_by_date[resolved_expiration] ||= {
            expiration_date: resolved_expiration,
            requested_buckets: []
          })
          item[:requested_buckets] << bucket
        end

        ordered_expirations = resolved_by_date.values.sort_by { |item| item[:expiration_date] }
        partition_into_balanced_groups(ordered_expirations, batch_size).map do |group|
          {
            symbol: entry.symbol,
            option_root: entry.option_root,
            provider_name: provider_name,
            from_date: group.first[:expiration_date],
            to_date: group.last[:expiration_date],
            expirations: group
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
          from_date: @expiration_date,
          to_date: @expiration_date,
          expirations: [
            {
              expiration_date: @expiration_date,
              requested_buckets: [requested_bucket]
            }
          ]
        }
      end
    end

    def partition_into_balanced_groups(items, batch_size)
      return [] if items.empty?

      effective_batch_size = [batch_size.to_i, 1].max
      group_count = (items.length.to_f / effective_batch_size).ceil
      base_size = items.length / group_count
      remainder = items.length % group_count

      groups = []
      offset = 0
      group_count.times do |idx|
        size = base_size + (idx < remainder ? 1 : 0)
        groups << items[offset, size]
        offset += size
      end
      groups
    end

    def fetch_expiration_chain(client, symbol)
      chain = with_retries("option expiration chain for #{symbol}") do
        client.get_option_expiration_chain(symbol)
      end
      raise Tickrake::Error, "Option expiration chain request returned nil for #{symbol}" if chain.nil?
      raise Tickrake::Error, "Unexpected option expiration chain result for #{symbol}: #{chain.class}" unless chain.respond_to?(:expiration_list)

      chain
    end

    def resolve_expiration(expiration_chain, bucket, option_root)
      all_expirations = Array(expiration_chain.expiration_list)
      expiration = all_expirations.find do |candidate|
        candidate.days_to_expiration == bucket && root_matches?(candidate, option_root)
      end

      return expiration if expiration

      available_dtes = all_expirations.map(&:days_to_expiration).sort.first(10)
      available_roots = all_expirations.flat_map { |e| expiration_roots(e) }.uniq.sort
      @runtime.logger.info(
        "Skipping option fetch bucket=#{bucket} root=#{option_root || '-'} — no matching expiration. " \
        "Schwab returned #{all_expirations.length} expirations with DTEs=#{available_dtes.inspect} roots=#{available_roots.inspect}"
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

    def process_queue(queue, run_time, collection_id)
      index = 0
      mutex = Mutex.new
      worker_count = [@runtime.config.max_workers, queue.length].min
      success_count = 0
      failure_count = 0

      Array.new(worker_count) do
        Thread.new do
          loop do
            job = mutex.synchronize do
              current = queue[index]
              index += 1 if current
              current
            end
            break unless job

            # Scheduled-run resilience treats any per-fetch failure as a degraded
            # iteration, even though the job continues processing the remaining queue.
            outcome = fetch_one(job, run_time, collection_id)
            mutex.synchronize do
              if outcome == :success
                success_count += 1
              else
                failure_count += 1
              end
            end
          end
        end
      end.each(&:join)

      ScheduledRunResult.new(success_count: success_count, failure_count: failure_count)
    end

    def selected_universe
      @universe || @scheduled_job&.universe || @runtime.config.options_universe
    end

    def selected_dte_buckets
      @scheduled_job&.dte_buckets || @runtime.config.dte_buckets
    end

    def selected_batch_size
      raw = if @scheduled_job&.settings.is_a?(Hash)
              @scheduled_job.settings["batch_size"] || @scheduled_job.settings[:batch_size]
            else
              first_options_job = @runtime.config.jobs_by_type("options").first
              if first_options_job&.settings.is_a?(Hash)
                first_options_job.settings["batch_size"] || first_options_job.settings[:batch_size]
              end
            end
      [raw.to_i, 1].max
    end

    def fetch_one(job, run_time, collection_id)
      symbol = job.fetch(:symbol)
      from_date = job.fetch(:from_date)
      to_date = job.fetch(:to_date)
      expirations = job.fetch(:expirations)
      provider_name = job.fetch(:provider_name)
      option_root = job[:option_root]
      exp_label = from_date == to_date ? from_date.to_s : "#{from_date}..#{to_date}"
      requested_bucket = expirations.flat_map { |e| e.fetch(:requested_buckets) }.join(",")

      @runtime.logger.info({
        msg: "fetch_start",
        event: "fetch_start",
        symbol: symbol,
        option_root: option_root,
        bucket: requested_bucket,
        expiration_date: exp_label,
        collection_id: collection_id
      })
      started_at = Time.now
      begin
        result = with_retries("#{symbol} exp=#{exp_label}", on_retry: ->(attempt, error) {
          @runtime.logger.warn({ msg: "fetch_retry", event: "fetch_retry", symbol: symbol, expiration_date: exp_label, attempt: attempt, error_class: error.class.name, error_message: error.message })
        }) do
          write_option_chains(
            client: client,
            provider_name: provider_name,
            symbol: symbol,
            from_date: from_date,
            to_date: to_date,
            expirations: expirations,
            timestamp: run_time,
            root: option_root
          )
        end
        elapsed_ms = ((Time.now - started_at) * 1000).round
        is_chain_truncated = result.fetch(:is_chain_truncated)

        if is_chain_truncated
          @runtime.logger.warn({
            msg: "option_chain_truncated",
            event: "option_chain_truncated",
            symbol: symbol,
            option_root: option_root,
            expiration_date: exp_label,
            is_chain_truncated: true,
            collection_id: collection_id
          })
        end

        result.fetch(:outputs).each do |output|
          exp_date = output.fetch(:expiration_date)
          @runtime.logger.info({
            msg: "fetch_success",
            event: "fetch_success",
            data_type: "options",
            provider: provider_name,
            symbol: symbol,
            option_root: option_root,
            expiration_date: exp_date.to_s,
            market_date: market_date_for(run_time),
            row_count: output.fetch(:row_count),
            byte_count: output.fetch(:byte_count),
            is_chain_truncated: is_chain_truncated,
            duration_ms: elapsed_ms,
            path: output.fetch(:path),
            collection_id: collection_id
          })
          write_sidecar(
            job: job.merge(
              expiration_date: exp_date,
              requested_buckets: output.fetch(:requested_buckets)
            ),
            run_time: run_time,
            collection_id: collection_id,
            started_at: started_at,
            status: "success",
            output_path: output.fetch(:path),
            row_count: output.fetch(:row_count)
          )
          @progress_reporter&.advance(title: option_progress_title(symbol, option_root, exp_date))
        end
        :success
      rescue StandardError, Timeout::ExitException => e
        elapsed_ms = ((Time.now - started_at) * 1000).round
        http_status = e.respond_to?(:response) ? e.response&.status : nil
        expirations.each do |exp_item|
          exp_date = exp_item.fetch(:expiration_date)
          @runtime.logger.error({ msg: "fetch_failed", event: "fetch_failed", symbol: symbol, option_root: option_root, expiration_date: exp_date.to_s, error_class: e.class.name, error_message: e.message, http_status: http_status, duration_ms: elapsed_ms, collection_id: collection_id })
          write_sidecar(
            job: job.merge(
              expiration_date: exp_date,
              requested_buckets: exp_item.fetch(:requested_buckets)
            ),
            run_time: run_time,
            collection_id: collection_id,
            started_at: started_at,
            status: "failed",
            error_message: "#{http_status ? "HTTP #{http_status} " : ""}#{e.message}".strip
          )
          @progress_reporter&.advance(title: "#{option_progress_title(symbol, option_root, exp_date)} failed")
        end
        :failed
      end
    end

    def option_progress_title(symbol, option_root, expiration_date)
      [symbol, option_root, expiration_date.iso8601].compact.reject(&:empty?).join(" ")
    end

    def market_date_for(timestamp)
      tz_name = @runtime.config.timezone.to_s
      return timestamp.utc.to_date.iso8601 if tz_name.empty? || tz_name.casecmp("utc").zero?

      TZInfo::Timezone.get(tz_name).utc_to_local(timestamp.utc).to_date.iso8601
    end

    def write_option_chains(client:, provider_name:, symbol:, from_date:, to_date:, expirations:, timestamp:, root:)
      rate_limiter_for(provider_name)&.consume!
      chain = Timeout.timeout(@runtime.config.option_fetch_timeout_seconds) do
        client.get_option_chain(
          self.class.option_chain_api_symbol(symbol),
          contract_type: SchwabRb::Option::ContractTypes::ALL,
          strike_range: SchwabRb::Option::StrikeRanges::ALL,
          from_date: from_date,
          to_date: to_date
        )
      end
      exp_label = from_date == to_date ? from_date.to_s : "#{from_date}..#{to_date}"
      raise Tickrake::Error, "Option chain request returned nil for #{symbol} exp=#{exp_label}" if chain.nil?

      is_chain_truncated = chain.respond_to?(:is_chain_truncated) ? (chain.is_chain_truncated == true) : false
      single_expiration = expirations.length == 1

      outputs = expirations.map do |exp_item|
        expiration_date = exp_item.fetch(:expiration_date)
        rows = option_sample_rows(
          chain,
          root,
          provider_name,
          symbol: symbol,
          timestamp: timestamp,
          expiration_date: expiration_date,
          single_expiration: single_expiration
        )
        path = storage_paths.option_sample_path(
          provider: provider_name,
          symbol: symbol,
          expiration_date: expiration_date,
          timestamp: timestamp,
          root: root
        )
        option_sample_writer.write(path: path, rows: rows)
        byte_count = File.size(path)

        {
          expiration_date: expiration_date,
          requested_buckets: exp_item.fetch(:requested_buckets),
          path: path,
          row_count: rows.length,
          byte_count: byte_count
        }
      end

      { is_chain_truncated: is_chain_truncated, outputs: outputs }
    end

    def option_sample_rows(chain, option_root, provider_name, symbol: nil, timestamp: Time.now.utc, expiration_date: nil, single_expiration: true)
      batch_time = timestamp.utc
      filtered_options(chain, option_root, expiration_date: expiration_date, single_expiration: single_expiration).sort_by do |option|
        [
          option.expiration_date&.iso8601.to_s,
          option.put_call.to_s,
          option.strike.to_f
        ]
      end.map do |option|
        resolved_root = option_root ||
                        (option.respond_to?(:option_root) && option.option_root) ||
                        (option.respond_to?(:underlying_symbol) && option.underlying_symbol) ||
                        symbol
        resolved_underlying = symbol ||
                              (chain.respond_to?(:symbol) && chain.symbol) ||
                              (option.respond_to?(:underlying_symbol) && option.underlying_symbol)

        Tickrake::Data::OptionSampleRow.new(
          contract_type: option.put_call,
          symbol: option.symbol,
          underlying_symbol: resolved_underlying,
          option_root: resolved_root,
          description: option.description,
          strike: option.strike,
          expiration_date: option.expiration_date&.iso8601,
          open: option.respond_to?(:open_price) ? option.open_price : nil,
          high: option.respond_to?(:high_price) ? option.high_price : nil,
          low: option.respond_to?(:low_price) ? option.low_price : nil,
          close: option.respond_to?(:close_price) ? option.close_price : nil,
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
          quote_at: format_epoch_ms(option.respond_to?(:quote_time_in_long) ? option.quote_time_in_long : nil) || batch_time.iso8601,
          trade_at: format_epoch_ms(option.respond_to?(:trade_time_in_long) ? option.trade_time_in_long : nil),
          time_value: option.respond_to?(:time_value) ? option.time_value : nil,
          net_change: option.respond_to?(:net_change) ? option.net_change : nil,
          percent_change: option.respond_to?(:percent_change) ? option.percent_change : nil,
          settlement_type: option.respond_to?(:settlement_type) ? option.settlement_type : nil,
          exercise_type: option.respond_to?(:exercise_type) ? option.exercise_type : nil,
          expiration_type: option.respond_to?(:expiration_type) ? option.expiration_type : nil,
          multiplier: option.respond_to?(:multiplier) ? option.multiplier : nil,
          non_standard: option.respond_to?(:non_standard) ? option.non_standard : nil,
          source: provider_name,
          sampled_at: batch_time.iso8601
        )
      end
    end

    def format_epoch_ms(ms)
      return nil if ms.nil? || ms.to_i.zero?

      Time.at(ms.to_f / 1000.0).utc.iso8601
    end

    def filtered_options(chain, option_root, expiration_date: nil, single_expiration: true)
      options = Array(chain.call_opts) + Array(chain.put_opts)
      unless option_root.nil? || option_root.empty?
        options = options.select { |option| option.option_root.to_s.upcase == option_root.to_s.upcase }
      end
      return options if expiration_date.nil?

      options.select do |option|
        opt_exp = option.respond_to?(:expiration_date) ? option.expiration_date : nil
        if opt_exp.nil?
          single_expiration
        else
          opt_date = opt_exp.is_a?(Date) ? opt_exp : Date.parse(opt_exp.to_s)
          opt_date == expiration_date
        end
      end
    end

    def storage_paths
      @storage_paths ||= Storage::Paths.new(@runtime.config)
    end

    def option_sample_writer
      @option_sample_writer ||= Storage::OptionSampleWriter.new
    end

    def rate_limiter_for(provider_name)
      @rate_limiters ||= {}
      return @rate_limiters[provider_name] if @rate_limiters.key?(provider_name)

      provider_def = @runtime.config.provider_definition(provider_name)
      max_req = provider_def.rate_limit_max_requests
      interval = provider_def.rate_limit_interval_seconds

      @rate_limiters[provider_name] = if max_req && interval
        Tickrake::RateLimiter.build(
          @runtime.config,
          provider: provider_name,
          capacity: max_req,
          refill_rate: max_req.to_f / interval
        )
      end
    end

    def provider_name_for(entry)
      provider_name = @runtime.config.provider_name_for_entry_with_override(
        @runtime.provider_override_name,
        entry,
        scheduled_job: @scheduled_job
      )
      provider_definition = @runtime.config.provider_definition(provider_name)
      unless provider_definition.adapter == "schwab"
        raise Tickrake::Error, "OptionsJob currently supports provider=schwab only (got #{provider_name})."
      end

      provider_name
    end

    def client
      @runtime.client_factory.build
    end

    def write_sidecar(job:, run_time:, collection_id:, started_at:, status:, output_path: nil, error_message: nil, row_count: nil)
      ticker = job[:option_root] || job.fetch(:symbol)
      ts = run_time.utc.strftime("%Y%m%dT%H%M%SZ")
      exp = job.fetch(:expiration_date).iso8601

      fetch_run = {
        "job_type" => @scheduled_job&.name || "options",
        "dataset_type" => "options",
        "symbol" => job.fetch(:symbol),
        "frequency" => nil,
        "option_root" => job[:option_root],
        "requested_buckets" => job.fetch(:requested_buckets),
        "resolved_expiration" => exp,
        "scheduled_for" => run_time.utc.iso8601,
        "started_at" => started_at.utc.iso8601,
        "finished_at" => Time.now.utc.iso8601,
        "status" => status,
        "output_path" => output_path,
        "error_message" => error_message,
        "collection_id" => collection_id
      }

      file_metadata = nil
      if status == "success" && output_path
        stat = File.stat(output_path)
        observed_at = run_time.utc.iso8601
        file_metadata = {
          "path" => output_path,
          "dataset_type" => "options",
          "provider_name" => job.fetch(:provider_name),
          "ticker" => ticker,
          "frequency" => nil,
          "expiration_date" => exp,
          "row_count" => row_count,
          "first_observed_at" => observed_at,
          "last_observed_at" => observed_at,
          "file_mtime" => stat.mtime.to_i,
          "file_size" => stat.size,
          "updated_at" => Time.now.utc.iso8601,
          "collection_id" => collection_id
        }
      end

      sidecar = { "fetch_run" => fetch_run, "file_metadata" => file_metadata }
      pending_dir = @runtime.config.pending_metadata_dir
      FileUtils.mkdir_p(pending_dir)
      basename = "#{ticker}_exp#{exp}_#{ts}.meta.json"
      sidecar_path = File.join(pending_dir, basename)
      tmp_path = "#{sidecar_path}.tmp"
      File.write(tmp_path, JSON.generate(sidecar))
      File.rename(tmp_path, sidecar_path)
    end

    def with_retries(label, on_retry: nil)
      attempts = 0

      begin
        attempts += 1
        Timeout.timeout(@runtime.config.option_fetch_timeout_seconds) { yield }
      rescue StandardError, Timeout::ExitException => e
        if attempts <= @runtime.config.retry_count
          on_retry&.call(attempts, e)
          sleep @runtime.config.retry_delay_seconds
          retry
        end

        if on_retry.nil?
          http_status = e.respond_to?(:response) ? " HTTP #{e.response&.status}" : ""
          @runtime.logger.error("Exhausted retries for #{label}:#{http_status} #{e.message}")
        end
        raise
      end
    end
  end
end
